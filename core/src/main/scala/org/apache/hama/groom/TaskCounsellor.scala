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
import org.apache.hama.monitor.SlotStats
import scala.collection.immutable.Queue
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

object TaskFailure {

  def apply(id: TaskAttemptID, stats: GroomStats): TaskFailure = {
    val fault = new TaskFailure
    fault.id = Option(id)
    fault
  }

}

protected[groom] final class TaskFailure extends Writable {

  protected[groom] var id: Option[TaskAttemptID] = None

  protected[groom] var s: Option[GroomStats] = None

  def taskAttemptId(): Option[TaskAttemptID] = id

  def stats(): Option[GroomStats] = s

  @throws(classOf[IOException])
  override def write(out: DataOutput) = {
    id.map { v => v.write(out) } 
    s.map { v => v.write(out) }
  }

  @throws(classOf[IOException])
  override def readFields(in: DataInput) {
    val v = new TaskAttemptID
    v.readFields(in)
    id = Option(v)
    val gs = new GroomStats
    gs.readFields(in)
    s = Option(gs)
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

  override val supervisorStrategy = OneForOneStrategy() {
    case ee: ExecutorException => {
      unwatchContainerWith(ee.slotSeq)
      Stop
    }
  }

  protected val slotManager = SlotManager(defaultMaxTasks)

  //protected val slotListener = new SlotListener(setting)

  /**
   * The max size of slots can't exceed configured maxTasks.
   */
  protected var slots = Set.empty[Slot] //  TODO: REMOVE

  protected var retries = Map.empty[SlotId, RetryCount]

  /**
   * {@link Directive}s are stored in this queue if the corresponded container 
   * are not yet initialized. Once the container is initialized, directives 
   * will be dispatched to container directly.
   */
  //protected var directiveQueue = Queue.empty[SlotToDirective]
  protected var directiveQueue = Queue.empty[Directive]

  protected def unwatchContainerWith(seq: Int) = 
    slotManager.findThenMap({ slot => (slot.seq == seq)})({ found => 
      found.container.map { container => context unwatch container }
    }) 

  protected def defaultMaxTasks(): Int = 
    setting.hama.getInt("bsp.tasks.maximum", 3)

  protected def maxRetries(): Int = 
    setting.hama.getInt("groom.executor.max_retries", 3)

  override def initializeServices = if(requestTask) tick(self, TaskRequest)

  protected def deployContainer(seq: Int): Option[ActorRef] = { 
    setting.hama.setInt("container.slot.seq", seq) 
    val info = setting.info
    // TODO: move to trait ProcessManager#deploy(name, class, addr, setting)
    val addr = Address(info.getProtocol.toString, info.getActorSystemName, 
                       info.getHost, info.getPort)
    Option(context.actorOf(Props(classOf[Container], setting, seq, self).
                             withDeploy(Deploy(scope = RemoteScope(addr))),
                           Container.simpleName(setting.hama)))
  }  

  protected def requestTask(): Boolean = 
    setting.hama.getBoolean("groom.request.task", true) 

  /**
   * Periodically request to master for a new task when the groom has free slots
   * and no task in directive queue.
   */
  override def ticked(msg: Tick): Unit = msg match {
    case TaskRequest => 
      if((directiveQueue.size + slotManager.numSlotsOccupied) < 
          slotManager.maxTasksAllowed) 
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
    val maxTasksAllowed = slotManager.maxTasksAllowed
    val slotStats = currentSlotStats
    LOG.debug("Current groom stats: name {}, host {}, port {}, maxTasks {}, "+
              "slot stats {}", name, host, port, maxTasksAllowed, slotStats)
    GroomStats(name, host, port, maxTasksAllowed, slotStats)
  } 

  protected def currentSlotStats(): SlotStats = {
    SlotStats.defaultSlotStats(setting)
    // TODO: 
  }

  protected def whenExecutorNotFound(oldSlot: Slot, d: Directive) {
    slotManager.update(oldSlot.seq, None, d.master, 
                       Option(newExecutor(oldSlot.seq)), None)
    //directiveQueue = directiveQueue.enqueue(SlotToDirective(oldSlot.seq, d))
    directiveQueue = directiveQueue.enqueue(d)
  }

  protected def dispatch(d: Directive)(slot: Slot) = d.action match {
    case Launch => slot.container.map { container => 
      container ! new LaunchTask(d.task) 
      slotManager.book(slot.seq, d.task.getId, container) 
    }
    case Resume => slot.executor.map { container => 
      container ! new ResumeTask(d.task) 
      slotManager.book(slot.seq, d.task.getId, container)
    }
    case Kill => // won't be here
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
      found => whenExecutorNotFound(found, d)
    })({ whenExecutorExists(d) })

  // TODO: when a task finishes, coordinator in container sends finishes msg 
  //       to task counsellor, then task counsellor updates slot taskAttemptId 
  //       to None

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
    case Kill => slotManager.findSlotBy(d.task.getId).map { slot =>
      slot.container match { 
        case Some(container) => container ! new KillTask(d.task.getId)  
        case None => LOG.error("No container is running for task {}!", 
                               d.task.getId)
      }
    }
    case _ => LOG.warning("Unknown directive {}", d)
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
  protected def doKillAck(action: KillAck) = 
    slotManager.clearSlotBy(action.slotSeq, action.taskAttemptId)

  protected def pushToLaunch(seq: Int, directive: Directive, from: ActorRef) {
    from ! new LaunchTask(directive.task)
    slotManager.book(seq, directive.task.getId, from)
  }

  protected def pushToResume(seq: Int, directive: Directive, from: ActorRef) {
    sender ! new ResumeTask(directive.task)
    slotManager.book(seq, directive.task.getId, from)
  }

  protected def messageFromCollector: Receive = { 
    case GetGroomStats => sender ! currentGroomStats
  }

  /**
   * When an executor fails, counsellor unwatch container so container won't 
   * be observed on offline; when only container fails (no executor), offline 
   * will notify accordingly.  
   */
  // TODO: move to ProcessManager trait?
  override def offline(from: ActorRef) = from.path.name match {
    case name if name.contains("_executor_") => name.split("_") match{
      case ary if (ary.size == 3) => extractSeq(ary(2), { seq => 
        matchSlot(seq, { slot => checkIfRetry(slot, { seq => 
          newExecutor(seq) })
        }) 
      })
      case _ => LOG.error("Invalid executor name", from.path.name)
    }
    case name if name.contains("Container") => 
      name.replace("Container", "") match {
        case s if s forall Character.isDigit => extractSeq(s, { seq =>
          matchSlot(seq, { slot => 
            slot.taskAttemptId.map { found => /* TODO: report to master */ }
            redeploy(slot)
          })
        })
        case s@_ => LOG.error("Invalid container {} offline!", name)
    }
    case _ => LOG.warning("Unknown supervisee {} offline!", from.path.name)
  }

  // TODO: move to ProcessManager trait?
  protected def redeploy(slot: Slot) {
    slotManager.clear(slot.seq)
    slotManager.update(slot.seq, slot.taskAttemptId, slot.master, slot.executor,
                       deployContainer(slot.seq))
  }

  protected def checkIfRetry(old: Slot, 
                             f:(Int) => Unit) = retries.get(old.seq) match {
      case Some(retryCount) if (retryCount < maxRetries) => { // need retry
         None.equals(old.taskAttemptId) match {
           case false => {
              // TODO: report to master
         
           }
           case true =>
         }
         // TODO: 
         // if task attempt id != none, 
         //    report task failure with id and latest groom stats to master  
         //    retry
         // else retry
/*
            slotManager.clear(old.seq)
            f(old.seq) 
            val v = retryCount + 1
            retries ++= Map(old.seq -> v)
*/
      }
      case Some(retryCount) if (retryCount >= maxRetries) => { // report
        //slots -= old 
        retries = retries.updated(old.seq, 0)   
        // TODO: report groom stats to master 
        //       black list?
        //       when slots.size == 0, notify groom to halt/ shutdown itself because no slot to run task? 
      }
      case None => { // 0 // need retry 
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
    case ProcessReady(seq) => deployContainer(seq).map { container => 
      slotManager.findSlotBy(seq).map { slot => 
        slotManager.update(seq, slot.taskAttemptId, slot.master, slot.executor,
                           Option(container)) 
      }
      dispatchDirective(seq, container)
    }
  }

  protected def dispatchDirective(seq: Int, container: ActorRef) = 
    if(!directiveQueue.isEmpty) {
      LOG.info("{} is asking for task execution...", container.path.name)
      val (d, rest) = directiveQueue.dequeue
      d.action match {
        case Launch => {
          LOG.info("{} requests for LaunchTask.", container.path.name)
          pushToLaunch(seq, d, container)  
        }
        case Kill => LOG.error("Container {} shouldn't request kill ",
                               "action!", container.path.name)
        case Resume => {
          LOG.info("{} requests for ResumeTask.", container.path.name)
          pushToResume(seq, d, container)
        }
        case _ => LOG.warning("Unknown action {} for task {} from {}", 
                              d.action, d.task.getId, d.master)
      }
      directiveQueue = rest
    }

  override def receive = processReady orElse tickMessage orElse messageFromCollector orElse killAck orElse receiveDirective orElse superviseeOffline orElse unknown

}
