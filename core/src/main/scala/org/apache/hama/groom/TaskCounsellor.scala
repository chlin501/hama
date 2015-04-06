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

import akka.actor.ActorInitializationException
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
import org.apache.hama.HamaConfiguration
import org.apache.hama.Periodically
import org.apache.hama.Service
import org.apache.hama.SubscribeEvent
import org.apache.hama.Spawnable
import org.apache.hama.UnsubscribeEvent
import org.apache.hama.Tick
import org.apache.hama.bsp.TaskAttemptID
import org.apache.hama.bsp.v2.Task
import org.apache.hama.bsp.v2.TaskFinished
import org.apache.hama.conf.Setting
import org.apache.hama.logging.CommonLog
import org.apache.hama.master.Directive
import org.apache.hama.master.Directive.Action._
import org.apache.hama.master.TaskCancelled
import org.apache.hama.monitor.CollectedStats
import org.apache.hama.monitor.GetGroomStats
import org.apache.hama.monitor.GroomStats
import org.apache.hama.monitor.GroomStats._
import org.apache.hama.monitor.PublishMessage
import org.apache.hama.monitor.SlotStats
import org.apache.hama.monitor.groom.GroomStatsCollector
import org.apache.hama.monitor.groom.TaskStatsCollector
import scala.collection.immutable.Queue
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

object TaskFailure {

  def apply(id: TaskAttemptID, stats: GroomStats): TaskFailure = {
    require(null != id && null != stats , 
            "TaskAttemptID or GroomStats is null!")
    val fault = new TaskFailure
    fault.id = id
    fault.s = stats
    fault
  } 

}

final class TaskFailure extends Writable {

  protected[groom] var id: TaskAttemptID = new TaskAttemptID 

  protected[groom] var s: GroomStats = new GroomStats

  def taskAttemptId(): TaskAttemptID = id

  def stats(): GroomStats = s

  @throws(classOf[IOException])
  override def write(out: DataOutput) = {
    id.write(out) 
    s.write(out)
  }

  @throws(classOf[IOException])
  override def readFields(in: DataInput) {
    id = new TaskAttemptID
    id.readFields(in)
    s = new GroomStats
    s.readFields(in)
  }
}

object NoFreeSlot {

  def apply(d: Directive): NoFreeSlot = {
    val sign = new NoFreeSlot
    sign.d = d
    sign
  }

}

final class NoFreeSlot extends Writable {

  protected[groom] var d: Directive = new Directive 
 
  def directive(): Directive = d

  override def write(out: DataOutput) = d.write(out)

  override def readFields(in: DataInput) = {
    var d = new Directive
    d.readFields(in) 
    this.d = d
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

  type TaskAttemptID = String

  override val supervisorStrategy = OneForOneStrategy() {
    case e: ActorInitializationException => e.getCause match {
      /**
       * Executor throws exception.
       */
      case ee: ExecutorException => { 
        LOG.error("Executor exception: {}", ee)
        unwatchContainerWith(ee.slotSeq)
        Stop 
      }
      case cause@_ => { 
        LOG.error("Fail creating child because {}", cause); Stop 
      }
    }
    case e: Exception => { 
      LOG.error("Unknown exception is thrown: {} ", e); Stop 
    }
  }

  protected val slotManager = SlotManager(defaultMaxTasks)

  protected var retries = Map.empty[SlotId, RetryCount]

  /**
   * {@link Directive}s are stored in this queue if the corresponded container 
   * are not yet initialized. Once the container is initialized, directives 
   * will be dispatched to container directly.
   */
  protected var directiveQueue = Queue.empty[Directive]

  protected def unwatchContainerWith(seq: Int) = 
    slotManager.findThenMap({ slot => (slot.seq == seq)})({ found => 
      found.container.map { container => context unwatch container }
    }) 

  protected def defaultMaxTasks(): Int = 
    setting.hama.getInt("bsp.tasks.maximum", 3)

  protected def maxRetries(): Int = 
    setting.hama.getInt("groom.executor.max_retries", 3)

  override def initializeServices = {
    groom ! SubscribeEvent(DirectiveArrivalEvent)// TODO: unsubscriveEvent(...)
    if(requestTask) tick(self, TaskRequest)
  }

  protected def deploy(sys: String, seq: Int, host: String, port: Int): 
    Option[ActorRef] = {  
      val conf = Setting.container(sys, seq, host, port)
      val info = conf.info
      // TODO: move to trait ProcessManager#deploy(name, class, addr, conf)
      val addr = Address(info.getProtocol.toString, info.getActorSystemName, 
                         info.getHost, info.getPort)
      val container = context.actorOf(Props(classOf[Container], sys, seq, host,
        port, self).withDeploy(Deploy(scope = RemoteScope(addr))), 
        Container.simpleName(conf.hama))
      context watch container
    Option(container)
  }  

  protected def requestTask(): Boolean = 
    setting.hama.getBoolean("groom.request.task", true) 

  /**
   * Periodically request to master for a new task when the groom has free slots
   * and no task in directive queue.
   */
  override def ticked(msg: Tick): Unit = msg match {
    case TaskRequest => if(slotManager.hasFreeSlot(directiveQueue.size))
      groom ! RequestTask(currentGroomStats) 
    case _ => 
  }
  
  //TODO: get sysload from collector periodically.


  /** 
   * Collect tasks information for reporting.
   * @return GroomServerStat contains the latest tasks statistics.
   */
  protected def currentGroomStats(): GroomStats = {
    val name = setting.name
    val host = setting.host
    val port = setting.port
    val maxTasksAllowed = slotManager.maxTasksAllowed - slotManager.brokenSlot
    val slotStats = currentSlotStats
    LOG.debug("Current groom stats: name {}, host {}, port {}, maxTasks {}, "+
              "slot stats {}", name, host, port, maxTasksAllowed, slotStats)
    GroomStats(name, host, port, maxTasksAllowed, slotStats)
  } 

  protected def currentSlotStats(): SlotStats = 
    SlotStats(slotManager.taskAttemptIdStrings, retries, maxRetries)

  protected def whenExecutorNotFound(oldSlot: Slot, d: Directive) { 
    slotManager.updateExecutor(oldSlot.seq, Option(newExecutor(oldSlot.seq)), 
                               d.master)
    directiveQueue = directiveQueue.enqueue(d)
  }

  protected def dispatch(d: Directive)(slot: Slot) = d.action match {
    case Launch => slot.container.map { container => {
      container ! new LaunchTask(d.task) 
      slotManager.book(slot.seq, d.task.getId, container) 
    }}
    case Resume => slot.executor.map { container => {
      container ! new ResumeTask(d.task) 
      slotManager.book(slot.seq, d.task.getId, container)
    }}
    case _ => LOG.error("Action Cancel by {} shouldn't be here for slot " +
                        "{} for container does not exist!", d, slot.seq)
  }

  protected def whenExecutorExists(d: Directive): Unit = 
    slotManager.find[Unit]({ slot => !None.equals(slot.container) })({
      found => dispatch(d)(found)
    })({ directiveQueue = directiveQueue.enqueue(d) })

  /**
   * Fork a process by executor if required; otherwise dispatch directive to 
   * the container that has no task being executed.
   */
  protected def initializeOrDispatch(d: Directive) = 
    slotManager.find[Unit]({ slot => None.equals(slot.executor) })({
      executorEmptySlot => whenExecutorNotFound(executorEmptySlot, d)
    })({ whenExecutorExists(d) })

  // TODO: move to ProcessManager trait?
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
      case _ => whenReceive(directive)(sender) 
    }
  }

  /**
   * Initialize child process if no executor found; otherwise dispatch action 
   * to child process directly.
   * When directive is cancelled, slot update will be done after ack is 
   * received.
   */
  protected def whenReceive(d: Directive)(from: ActorRef) =
    if(slotManager.hasFreeSlot(directiveQueue.size)) d.action match {
      case Launch | Resume => {
        initializeOrDispatch(d) 
      }
      case Cancel => slotManager.findSlotBy(d.task.getId) match { 
        case Some(slot) => slot.container match { 
          case Some(container) => container ! new CancelTask(d.task.getId)  
          case None => LOG.error("No container is up. Can't cancel task {}!",
                                 d.task.getId)
        }
        case None => LOG.warning("Task {} may be stopped with task failure "+
                                 "reported.", d.task.getId)
      }
    } else from ! NoFreeSlot(d)

  /**
   * Executor ack for Cancel action.
   * Verify corresponded task is with Cancel action and correct taskAttemptId.
   * @param Receive is partial function.
   */
  protected def cancelAck: Receive = {
    case action: CancelAck => {
      preCancelAck(action)
      doCancelAck(action)
      postCancelAck(action)
    }
  }

  protected def preCancelAck(ack: CancelAck) { }

  /**
   * Notify master the task with specific task attempt id is cancelled.
   */
  protected def postCancelAck(ack: CancelAck) { 
    groom ! TaskCancelled(ack.taskAttemptId.toString)
  }

  /**
   * - Find corresponded slot seq and task attempt id replied from 
   * {@link Container}.
   * - Update information by removing task recorded in {@link Slot}.
   * @param action is the CancelAck that contains {@link TaskAttemptID} and slot
   *               seq. 
   */
  protected def doCancelAck(action: CancelAck) = 
    slotManager.clearSlotBy(action.slotSeq, action.taskAttemptId)

  protected def pushToLaunch(seq: Int, directive: Directive, 
                             container: ActorRef) {
    container ! new LaunchTask(directive.task)
    slotManager.book(seq, directive.task.getId, container)
  }

  protected def pushToResume(seq: Int, directive: Directive, 
                             container: ActorRef) {
    container ! new ResumeTask(directive.task)
    slotManager.book(seq, directive.task.getId, container)
  }

  // TODO: export as trait in allowing collector to execute functions?
  protected def collectorMsgs: Receive = {  
    case msg: PublishMessage => reporter ! msg 
    case GetGroomStats => sender ! CollectedStats(currentGroomStats)
  }

  /**
   * When an executor fails, container is unwatched so no container offline  
   * event will be observed. Only container fails (no executor), offline 
   * will notify accordingly.  
   */
  // TODO: move to ProcessManager trait?
  override def offline(from: ActorRef) = from.path.name match {
    case name if name.contains("_executor_") => whenExecutorOffline(name)
    case name if name.contains("Container") => whenContainerOffline(name)
    case _ => LOG.warning("Unknown supervisee {} offline!", from.path.name)
  }

  // TODO: check if exceeding max retry?
  protected def whenExecutorOffline(name: String) = name.split("_") match {
    case ary if (ary.size == 3) => extractSeq(ary(2), { seq => 
      matchSlot(seq, { slot => checkIfRetry(slot, { seq => {
        LOG.warning("{} is offline, recreating a new one!", name)
        newExecutor(seq) 
      }})})
    })
    case _ => LOG.error("Invalid executor name", name)
  }

  protected def whenContainerOffline(name: String) = 
    name.replace("Container", "") match {
      case s if s forall Character.isDigit => extractSeq(s, { seq =>
        matchSlot(seq, { slot => {
          slot.taskAttemptId.map { found => 
            groom ! TaskFailure(found, currentGroomStats) 
          }
          redeploy(slot)
        }})})
      case s@_ => LOG.error("Invalid container {} offline!", name)
    }

  protected def resolve(ref: Option[ActorRef]): (String, String, Int) = 
    ref match {
      case Some(r) => {
        val sys = r.path.address.system
        val host = r.path.address.host.getOrElse("localhost")
        val port = r.path.address.port.getOrElse(61000)
        (sys, host, port)
      }
      case None => ("BSPSystem", "localhost", 61000)
    }

  // TODO: move to ProcessManager trait?
  protected def redeploy(slot: Slot) = { 
    val (sys, host, port) = resolve(slot.container) 
    deploy(sys, slot.seq, host, port).map { container =>
      slotManager.updateContainer(slot.seq, Option(container))
      dispatchDirective(slot.seq, container) 
    }
  }
 
  protected def checkIfRetry(old: Slot, 
                             f:(Int) => Unit) = retries.get(old.seq) match {
      case Some(retryCount) if (retryCount < maxRetries) => { 
        if(!None.equals(old.taskAttemptId)) 
          groom ! TaskFailure(old.taskAttemptId.getOrElse(null), 
                              currentGroomStats)
        slotManager.clear(old.seq)
        f(old.seq) 
        val v = retryCount + 1
        retries ++= Map(old.seq -> v)
      }
      case Some(retryCount) if (retryCount >= maxRetries) => { 
        slotManager.markAsBroken(old.seq)
        retries = retries.updated(old.seq, 0)   
        if(!None.equals(old.taskAttemptId)) 
          groom ! TaskFailure(old.taskAttemptId.getOrElse(null), 
                              currentGroomStats)
      }
      case None => { // 0 
        slotManager.clear(old.seq)
        f(old.seq)
        retries ++= Map(old.seq -> 1)
      }
    }

  protected def matchSlot(seq: Int, 
                          matched: (Slot) => Unit): Unit = 
    matchSlot(seq, { slot => matched(slot) }, { 
      LOG.error("No slot matched to seq {}", seq) 
    })

  protected def matchSlot(seq: Int, matched: (Slot) => Unit, 
                          notMatched: => Unit) = 
    slotManager.find[Unit]({ slot => (slot.seq == seq) })({
      found => matched(found)
    })({ notMatched })

  protected def extractSeq(seq: String, f:(Int) => Unit) = 
    Try(seq.toInt) match {
      case Success(seq) => f(seq)
      case Failure(cause) => LOG.error("Invalid executor seq {} for {}",
                                       seq, cause)
    }

  /**
   * Process, not <b>container</b>, replies when it's ready. Then following 
   * actions are executed:
   * - Deploy container to the process.
   * - Update slot with corresponded container. 
   * - Dispatch to container.
   * @return Receive is partial function.
   */
  protected def processReady: Receive = { 
    case ProcessReady(sys, seq, host, port) => 
      deploy(sys, seq, host, port).map { container => {
        slotManager.updateContainer(seq, Option(container)) 
        dispatchDirective(seq, container)
      }}
  }

  protected def dispatchDirective(seq: Int, container: ActorRef) = 
    if(!directiveQueue.isEmpty) {
      LOG.info("Direcive queue is not empty and container {} is asking for "+
               "task execution!", container.path.name)
      val (d, rest) = directiveQueue.dequeue
      d.action match {
        case Launch => {
          LOG.debug("{} requests for LaunchTask.", container.path.name)
          pushToLaunch(seq, d, container)  
        }
        case Resume => {
          LOG.debug("{} requests for ResumeTask.", container.path.name)
          pushToResume(seq, d, container)
        }
        case _ => LOG.warning("Container {} shouldn't request Cancel action!", 
                              container.path.name)
      }
      directiveQueue = rest
    } 

  protected def escalate: Receive = {
    case finished: TaskFinished => {
      groom ! finished
      slotManager.clearTaskAttemptId(finished.taskAttemptId)
    }
  }

  override def receive = escalate orElse collectorMsgs orElse processReady orElse tickMessage orElse cancelAck orElse receiveDirective orElse superviseeOffline orElse unknown

}
