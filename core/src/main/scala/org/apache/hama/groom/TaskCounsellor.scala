/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hama.groom

import akka.actor.ActorRef
import akka.actor.Address
import akka.actor.Deploy
import akka.actor.OneForOneStrategy
import akka.actor.Props
import akka.actor.SupervisorStrategy.Restart
import akka.actor.SupervisorStrategy.Stop
import akka.remote.RemoteScope
import java.io.DataInput
import java.io.DataOutput
import java.io.IOException
import org.apache.hadoop.io.Writable
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.recipes.cache.PathChildrenCache
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type._
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener
import org.apache.hama.HamaConfiguration
import org.apache.hama.Periodically
import org.apache.hama.Service
import org.apache.hama.Spawnable
import org.apache.hama.Tick
import org.apache.hama.bsp.v2.Task
import org.apache.hama.bsp.TaskAttemptID
import org.apache.hama.conf.Setting
import org.apache.hama.logging.CommonLog
import org.apache.hama.master.Scheduler
import org.apache.hama.master.Directive
import org.apache.hama.master.Directive.Action._
import org.apache.hama.monitor.GetGroomStats
import org.apache.hama.monitor.GroomStats
import org.apache.hama.monitor.GroomStats._
import org.apache.hama.util.Curator
import scala.collection.immutable.Queue
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

object TaskFailure {

  def apply(id: TaskAttemptID): TaskFailure = {
    val fault = new TaskFailure
    fault.id = Option(id)
    fault
  }

}

protected[groom] final class TaskFailure extends Writable {

  protected[groom] var id: Option[TaskAttemptID] = None

  def taskAttemptId(): Option[TaskAttemptID] = id

  @throws(classOf[IOException])
  override def write(out: DataOutput) = id.map { v => v.write(out) } 

  @throws(classOf[IOException])
  override def readFields(in: DataInput) {
    val v = new TaskAttemptID
    v.readFields(in)
    id = Option(v)
  }
  
}

protected[groom] class SlotListener(setting: Setting) extends Curator 
                                                         with CommonLog {

  protected var herald: Option[PathChildrenCache] = None

  protected[groom] def containerZkPath(): String = 
    "/container/%s:%s".format(setting.host, setting.port)

  protected[groom] def initialize() = herald = 
    initializeCurator(setting.hama).map { curator => 
      val h = new PathChildrenCache(curator, containerZkPath, false)
      h.start 
      h
    }

  protected[groom] def reactWith(callback: (String) => Unit) = herald.map { h =>
    h.getListenable.addListener(new PathChildrenCacheListener() {
      override def childEvent(curator: CuratorFramework, 
                              event: PathChildrenCacheEvent) = 
        event.getType match {
          case CHILD_ADDED => callback(event.getData.getPath)
          case rest@_ => LOG.debug("Event {} gets called!", rest)
        }
    })
  }
}

object TaskCounsellor {

  def simpleName(conf: HamaConfiguration): String = conf.get(
    "groom.taskcounsellor.name",
    classOf[TaskCounsellor].getSimpleName
  )

}

class TaskCounsellor(setting: Setting, groom: ActorRef, reporter: ActorRef) 
      extends Service with Spawnable with Periodically {

  type SlotId = Int

  type RetryCount = Int

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 1, withinTimeRange = 1 minute) {
      case ee: ExecutorException => {
        unwatchContainerWith(ee.slotSeq)
        Stop
      }
      case _: Exception => { // TODO: deloy container instead of creating one in remeoting 
        // TODO: check if it's container
        //       report master with task id  
        Restart
      }
    }

  protected val slotListener = new SlotListener(setting)

  /**
   * The max size of slots can't exceed configured maxTasks.
   */
  protected var slots = Set.empty[Slot]

  protected var retries = Map.empty[SlotId, RetryCount]

  /**
   * {@link Directive}s are stored in this queue if the corresponded container 
   * are not yet initialized. Once the container is initialized, directives 
   * will be dispatched to container directly.
   */
  //protected var directiveQueue = Queue.empty[SlotToDirective]
  protected var directiveQueue = Queue.empty[Directive]


  protected def unwatchContainerWith(seq: Int) = slots.find( slot => 
    (slot.seq == seq)).map { slot => slot.container.map { container => 
    context unwatch container 
  }}

  protected def defaultMaxTasks(): Int = 
    setting.hama.getInt("bsp.tasks.maximum", 3)

  protected def maxTasksAllowed(): Int = slots.size // no black list at the moment

  protected def maxRetries(): Int = 
    setting.hama.getInt("groom.executor.max_retries", 3)

  /**
   * Initialize slots with default slots value to 3, which comes from maxTasks,
   * or "bsp.tasks.maximum".
   * @param constraint of the slots can be created.
   */
  protected def initializeSlots(constraint: Int = 3) {
    for(seq <- 1 to constraint) {
      slots ++= Set(Slot(seq, None, "", None, None))
    }
    LOG.info("{} GroomServer slots are initialied.", constraint)
  }


  override def initializeServices = {
    if(requestTask) tick(self, TaskRequest)
    initializeSlots(defaultMaxTasks)
    slotListener.initialize
    slotListener.reactWith({ childPath => {
      val registeredSeq = childPath diff (slotListener.containerZkPath + "/")
      Try(registeredSeq.toInt) match {
        case Success(seq) => deployContainer(seq)  
        case Failure(cause) => 
          LOG.error("Invalid registered slot seq {} due to {} ", registeredSeq,
                    cause)
      }
    }})
  }

  protected def deployContainer(seq: Int) { 
    setting.hama.setInt("container.slot.seq", seq) 
    val info = setting.info
    // TODO: move to Deployable#deploy(name, class, addr, setting)
    val addr = Address(info.getProtocol.toString, info.getActorSystemName, 
                       info.getHost, info.getPort)
    context.actorOf(Props(classOf[Container], setting, seq).
                      withDeploy(Deploy(scope = RemoteScope(addr))),
                    Container.simpleName(setting.hama))
  }  

  protected def numSlotsOccupied(): Int = slots.count( slot => 
    !None.equals(slot.taskAttemptId)
  ) 

  protected def requestTask(): Boolean = 
    setting.hama.getBoolean("groom.request.task", true) 

  /**
   * Periodically request to master for a new task when the groom has free slots
   * and no task in directive queue.
   */
  override def ticked(msg: Tick): Unit = msg match {
    case TaskRequest => 
      if((directiveQueue.size + numSlotsOccupied) < maxTasksAllowed) 
        groom ! RequestTask(currentGroomStats) 
    case _ => 
  }
  
  //TODO: get sysload from collector periodically.

  /** 
   * Collect tasks information for report.
   * @return GroomServerStat contains the latest tasks statistics.
   */
  protected def currentGroomStats(): GroomStats = {
    val name = setting.name
    val host = setting.host
    val port = setting.port
    val slotIds = list(slots) 
    val maxTasksNow = maxTasksAllowed
    LOG.debug("Current groom stats: name {}, host {}, port {}, maxTasks {}, "+
              "slots ids {}", name, host, port, maxTasksNow, slotIds)
    GroomStats(name, host, port, maxTasksNow, slotIds)
  } 

  /**
   * Find if there is corresponded task running on a slot.
   * @param task is the target task to be killed.
   * @return Option[ActorRef] contains {@link Executor} if matched; otherwise
   *                          None is returned.
   */
  protected def findSlot(task: Task): Option[Slot] = slots.find( slot => 
    slot.taskAttemptId match {
      case Some(taskAttemptId) => taskAttemptId.equals(task.getId)
      case None => false
    }
  ) 

  protected def whenExecutorNotFound(oldSlot: Slot, d: Directive) {
    val newSlot = Slot(oldSlot.seq, None, d.master, 
                       Option(newExecutor(oldSlot.seq)), None)
    slots -= oldSlot
    slots += newSlot
    //directiveQueue = directiveQueue.enqueue(SlotToDirective(oldSlot.seq, d))
    directiveQueue = directiveQueue.enqueue(d)
  }

  protected def dispatch(d: Directive)(slot: Slot) = d.action match {
    case Launch => slot.container.map { container => 
      container ! new LaunchTask(d.task) 
      book(slot.seq, d.task.getId, container) 
    }
    case Resume => slot.executor.map { container => 
      container ! new ResumeTask(d.task) 
      book(slot.seq, d.task.getId, container)
    }
    case Kill => // won't be here
  }

  protected def whenExecutorExists(d: Directive) = slots.find( slot => 
    !None.equals(slot.container)
  ) match {
    case Some(foundSlot) => dispatch(d)(foundSlot)
    case None => directiveQueue = directiveQueue.enqueue(d) 
  }

  /**
   * Fork a process by executor if required; otherwise dispatch directive to 
   * the container that has no task being executed.
   */
  protected def initializeOrDispatch(d: Directive) = 
    slots.find( slot => None.equals(slot.executor)) match {
      case Some(foundSlot) => whenExecutorNotFound(foundSlot, d)
      case None => whenExecutorExists(d)
    }

  // TODO: when a task finishes, coordinator in container sends finishes msg 
  //       to task counsellor, then task counsellor updates slot taskAttemptId 
  //       to None

  protected def newExecutor(slotSeq: Int): ActorRef = { 
    val executorConf = new HamaConfiguration(setting.hama)
    executorConf.setInt("groom.executor.slot.seq", slotSeq)
    val executorName = Executor.simpleName(executorConf)
    val executor = spawn(executorName, classOf[Executor], setting, slotSeq)
    context watch executor
    executor
  }

  /**
   * Receive {@link Directive} from Scheduler, deciding what to do next.
   * @return Receive is partial function.
   */
  protected def receiveDirective: Receive = {
    case directive: Directive => directive match {
      case null => LOG.error("Directive from {} is null!", sender.path.name)
      case _ => {
        LOG.info("Receive directive for action: "+directive.action+" task: "+
                 directive.task.getId+" master: "+directive.master)
        directiveReceived(directive) 
      }
    }
  }

  /**
   * Initialize child process if no executor found; otherwise dispatch action 
   * to child process directly.
   * When directive is kill, slot update will be done after ack is received.
   */
  protected def directiveReceived(d: Directive) = d.action match {
    case Launch | Resume => initializeOrDispatch(d) 
    case Kill => findSlot(d.task) match {
      case Some(slot) => slot.container match { 
        case Some(container) => container ! new KillTask(d.task.getId)  
        case None => LOG.error("No container is running for task {}!", 
                               d.task.getId)
      }
      case None => LOG.warning("Ask to Kill task {}, but no corresponded "+
                               "slot found!", d.task.getId)
    }
    case _ => LOG.warning("Unknown directive {}", d)
  }

  protected def slotNotOccupied(slot: Slot, taskAttemptId: TaskAttemptID,
                                container: ActorRef) {
    val newSlot = Slot(slot.seq, Option(taskAttemptId), slot.master, 
                       slot.executor, Option(container))
    slots -= slot 
    slots += newSlot
  }

  /**
   * Book the slot with corresponded {@link Task} and {@link Executor}.
   * @param slotSeq indicates the <i>N</i>th slot.
   * @param task is the task being executed
   * @param container runs a task on a separated process.
   */
  protected def book(slotSeq: Int, taskAttemptId: TaskAttemptID, 
                     container: ActorRef) = slots.find( slot => 
    (slotSeq == slot.seq)
  ) match {
    case Some(slot) => slot.taskAttemptId match {
      case None => slotNotOccupied(slot, taskAttemptId, container)
      case Some(id) => 
        LOG.error("Task {} can't be booked at slot {} for other task {} "+
                  "exists!", taskAttemptId, slotSeq, id)
    }
    case None => LOG.error("Slot with seq {} not found for task {}", slotSeq,
                           taskAttemptId)
  }

  /**
   * Executor ack for Kill action.
   * Verify corresponded task is with Kill action and correct taskAttemptId.
   * @param Receive is partial function.
   */
  protected def killAck: Receive = {
    case action: KillAck => {
      preKillAck(action)
      doKillAck(action)
      postKillAck(action)
    }
  }

  protected def preKillAck(ack: KillAck) { }

  protected def postKillAck(ack: KillAck) { }

  /**
   * - Find corresponded slot seq and task attempt id replied from 
   * {@link Container}.
   * - Update information by removing task recorded in {@link Slot}.
   * @param action is the KillAck that contains {@link TaskAttemptID} and slot
   *               seq. 
   */
  protected def doKillAck(action: KillAck) = slots.find( slot => {
    val seqEquals = (slot.seq == action.slotSeq)  
    val idEquals = slot.taskAttemptId match {
      case Some(found) => found.equals(action.taskAttemptId)
      case None => false
    }
    seqEquals && idEquals
  }) match {
    case Some(oldSlot) => removeTaskAttemptIdFromSlot(oldSlot) // TODO: also inform reporter!!
    case None => LOG.error("Killed task {} not found for slot seq {}. "+
                           "Slots currently contain {}", action.taskAttemptId,
                           action.slotSeq, slots)
  }

  protected def removeTaskAttemptIdFromSlot(old: Slot) {
    val newSlot = Slot(old.seq, None, old.master, old.executor, old.container)
    slots -= old 
    slots += newSlot
  }

  protected def pullForLaunch(seq: Int, directive: Directive, from: ActorRef) {
    from ! new LaunchTask(directive.task)
    book(seq, directive.task.getId, from)
  }

  protected def pullForResume(seq: Int, directive: Directive, from: ActorRef) {
    sender ! new ResumeTask(directive.task)
    book(seq, directive.task.getId, from)
  }

  protected def messageFromCollector: Receive = { 
    case GetGroomStats => sender ! currentGroomStats
  }

  /**
   * When an executor fails, counsellor unwatch container so container won't 
   * be observed on offline; when only container fails (no executor), offline 
   * will notify accordingly.  
   */
  override def offline(from: ActorRef) = from.path.name match {
    case name if name.contains("_executor_") => from.path.name.split("_") match{
      case ary if (ary.size == 3) => extractSeq(ary(2), { seq => 
        matchSlot(seq, { slot => whenSlotFound(slot, { slot => 
          checkIfRetry(slot, { seq => newExecutor(seq) })
        })}) 
      })
      case _ => LOG.error("Invalid executor name", from.path.name)
    }
   // TODO: clean up matched slot. if no task running, new executor; otherwise update slot, report to master
    case name if name.contains("Container") => /*{
      val seq = from.path.name.replace("Container", "") 
      seq forall Character.isDigit {
        case true => extratSeq(seq, { seq => matchSlot(seq, { slot => 
           whenSlotFound(slot, { slot => checkIfRetry(slot => { seq => })}) 
         }) 
        })
        case false => LOG.error("Invalid Container name: {}", name)
      }
    }*/
    case _ => LOG.warning("Unknown supervisee {} offline!", from.path.name)
  }

  protected def cleanupSlot(old: Slot) = slots.map { slot => 
    if(slot.seq == old.seq) {
      slots -= slot
      val newSlot = Slot(old.seq, None, old.master, None, None)
      slots += newSlot
  }}

  /**
   * WhenSlotFound function already reports task failure event. This function
   * only deals with retry action.
   */ 
  protected def checkIfRetry(old: Slot, 
                             f:(Int) => Unit) = retries.get(old.seq) match {
    case Some(retryCount) if (retryCount < maxRetries) => {
      cleanupSlot(old)
      f(old.seq) 
      val v = retryCount + 1
      retries ++= Map(old.seq -> v)
    }
    case Some(retryCount) if (retryCount >= maxRetries) => {
      slots -= old 
      retries = retries.updated(old.seq, 0)   
      // TODO: report groom stats to master 
      //       black list?
      //       when slots.size == 0, notify groom to halt/ shutdown itself because no slot to run task? 
    }
    case None => { // 0
      cleanupSlot(old)
      f(old.seq)
      retries ++= Map(old.seq -> 1)
    }
  }

  /**
   * If a task attempt id is found in slot, denoting a task running at that
   * container fails, task counsellor notify schedule that a task fails; 
   * otherwise respawning executor.
   */
  protected def whenSlotFound(slot: Slot, 
                              retry:(Slot) => Unit) = slot.taskAttemptId match {
    case Some(runningTaskId) => { 
      groom ! TaskFailure(runningTaskId)
      retry(slot)
    }
    case None => retry(slot) // whether directives in queue or not, restart execturor
  }

  protected def matchSlot(seq: Int, 
                          matched: (Slot) => Unit): Unit = 
    matchSlot(seq, { slot => matched(slot) }, { 
      LOG.error("No slot matched to seq {}", seq) 
    })

  protected def matchSlot(seq: Int, 
                          matched: (Slot) => Unit, 
                          notMatched: => Unit) = slots.find( slot => 
    (slot.seq == seq) 
  ) match {
    case Some(found) => matched(found)
    case None => notMatched
  }

  protected def extractSeq(seq: String, f:(Int) => Unit) = 
    Try(seq.toInt) match {
      case Success(seq) => f(seq)
      case Failure(cause) => LOG.error("Invalid executor seq {} for {}",
                                       seq, cause)
    }

  /**
   * Container replies when it's ready.
   * @return Receive is partial function.
   */
  protected def containerReady: Receive = {  
    case ContainerReady(seq) => {
      val container = sender
      context watch container
      if(!directiveQueue.isEmpty) {
        LOG.info("{} is asking for task execution...", container.path.name)
        val (d, rest) = directiveQueue.dequeue
        d.action match {
          case Launch => {
            LOG.info("{} requests for LaunchTask.", container.path.name)
            pullForLaunch(seq, d, container)  
          }
          case Kill => LOG.error("Container {} shouldn't request kill ",
                                 "action!", container.path.name)
          case Resume => {
            LOG.info("{} requests for ResumeTask.", container.path.name)
            pullForResume(seq, d, container)
          }
          case _ => LOG.warning("Unknown action {} for task {} from {}", 
                                d.action, d.task.getId, d.master)
        }
        directiveQueue = rest
      }
    }
  }

  override def receive = containerReady orElse tickMessage orElse messageFromCollector orElse killAck orElse receiveDirective orElse superviseeOffline orElse unknown

}
