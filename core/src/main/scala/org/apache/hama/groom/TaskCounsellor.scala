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
import org.apache.hama.master.Scheduler
import org.apache.hama.master.Directive
import org.apache.hama.master.Directive.Action._
import org.apache.hama.monitor.GetGroomStats
import org.apache.hama.monitor.GroomStats
import org.apache.hama.monitor.GroomStats._
import scala.collection.immutable.Queue
import scala.util.Failure
import scala.util.Success
import scala.util.Try

trait TaskCounsellorMessage
final case class SlotToDirective(seq: Int, directive: Directive)
      extends TaskCounsellorMessage

object TaskFailure {

  def apply(id: TaskAttemptID): TaskFailure = {
    val fault = new TaskFailure
    fault.id = Option(id)
    fault
  }

}

protected[groom] final class TaskFailure extends Writable 
                                         with TaskCounsellorMessage {
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

  /**
   * The max size of slots can't exceed configured maxTasks.
   */
  protected var slots = Set.empty[Slot]

  protected var retries = Map.empty[SlotId, RetryCount]

  /**
   * {@link Directive}s are stored in this queue when executors are not yet
   * initialized. Once executors are all initialized, directives will be
   * dispatched to executor directly.
   */
  protected var directiveQueue = Queue.empty[SlotToDirective]

  protected def maxTasks(): Int = setting.hama.getInt("bsp.tasks.maximum", 3)

  protected def maxRetries(): Int = 
    setting.hama.getInt("groom.executor.max_retries", 3)

  /**
   * Initialize slots with default slots value to 3, which comes from maxTasks,
   * or "bsp.tasks.maximum".
   * @param constraint of the slots can be created.
   */
  protected def initializeSlots(constraint: Int = 3) {
    for(seq <- 1 to constraint) {
      slots ++= Set(Slot(seq, None, "", None))
    }
    LOG.info("{} GroomServer slots are initialied.", constraint)
  }

  override def initializeServices = {
    if(requestTask) tick(self, TaskRequest)
    initializeSlots(maxTasks)
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
    case TaskRequest => if((directiveQueue.size + numSlotsOccupied) < maxTasks)
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
    LOG.debug("Current groom stats: name {}, host {}, port {}, maxTasks {}, "+
              "slots ids {}", name, host, port, maxTasks, slotIds)
    GroomStats(name, host, port, maxTasks, slotIds)
  } 

  /**
   * Find if there is corresponded task running on a slot.
   * @param task is the target task to be killed.
   * @return Option[ActorRef] contains {@link Executor} if matched; otherwise
   *                          None is returned.
   */
  protected def findTarget(task: Task): Option[Slot] = slots.find( slot => 
    slot.taskAttemptId match {
      case Some(taskAttemptId) => taskAttemptId.equals(task.getId)
      case None => false
    }
  ) 

  /**
   * Initialize an executor if needed; otherwise dispatch directive to the 
   * executor that has no task being executed.
   */
  protected def initializeOrDispatch(d: Directive) = shouldNewExecutor match {
    case true => slots.find( slot => None.equals(slot.executor)) match {
      case Some(found) => {
        val newSlot = Slot(found.seq, None, d.master, 
                           Option(newExecutor(found.seq)))
        slots -= found
        slots += newSlot
        directiveQueue = directiveQueue.enqueue(SlotToDirective(found.seq, d))
      }
      case None =>
    }
    case false => slots.find( slot => None.equals(slot.taskAttemptId)) match {
      case Some(found) => d.action match {
        case Launch => found.executor.map { e => 
          e ! new LaunchTask(d.task) 
          book(found.seq, d.task.getId)
        }
        case Resume => found.executor.map { e => 
          e ! new ResumeTask(d.task) 
          book(found.seq, d.task.getId)
        }
        case Kill => // won't be here
      }
      case None =>
    }
  }

  // TODO: when a task finishes, coordinator in container sends finishes msg 
  //       to task counsellor, then task counsellor updates slot taskAttemptId 
  //       to None

  protected def shouldNewExecutor(): Boolean = slots.exists( slot =>
    None.equals(slot.executor)
  )

  protected def newExecutor(slotSeq: Int): ActorRef = { 
    val executorName = Executor.simpleName(setting.hama, slotSeq)
    val executor = spawn(executorName, classOf[Executor], setting, slotSeq,
                         self) 
    context watch executor
    LOG.info("Executor for slot seq {} is spawned and watched.", slotSeq)
    executor
  }

  /**
   * Receive {@link Directive} from Scheduler, deciding what to do next.
   * @return Receive is partial function.
   */
  protected def receiveDirective: Receive = {
    case directive: Directive => directive match {
      case null => LOG.error("Directive dispatched from {} is null!", 
                             sender.path.name)
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
    case Kill => findTarget(d.task) match {
      case Some(slot) => slot.executor.map { e => 
        e ! new KillTask(d.task.getId) 
      }
      case None => LOG.warning("Ask to Kill task {}, but no corresponded "+
                               "executor found!", d.task.toString)
    }
    case _ => LOG.warning("Unknown directive {}", d)
  }

  /**
   * Book the slot with corresponded {@link Task} and {@link Executor}.
   * @param slotSeq indicates the <i>N</i>th slot.
   * @param task is the task being executed
   * @param executor is the executor that runs the task.
   */
  protected def book(slotSeq: Int, taskAttemptId: TaskAttemptID) = 
    slots.find( slot => (slotSeq == slot.seq)) match {
      case Some(slot) => slot.taskAttemptId match {
        case None => {
          val newSlot = Slot(slot.seq, Option(taskAttemptId), slot.master, 
                             slot.executor)
          slots -= slot 
          slots += newSlot
        }
        case Some(found) => 
          throw new RuntimeException("Task "+found+" can't be booked at slot "+
                                     slotSeq+" for the task "+found+" exists!")
      }
      case None => throw new RuntimeException("Slot with seq "+slotSeq+
                                              " not found for task "+
                                              taskAttemptId+"!")
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
    case Some(oldSlot) => cleanupSlot(oldSlot) // TODO: also inform reporter!!
    case None => LOG.warning("Killed task {} not found for slot seq {}. "+
                             "Slots currently contain {}", action.taskAttemptId,
                             action.slotSeq, slots)
  }

  protected def cleanupSlot(old: Slot) {
    val newSlot = Slot(old.seq, None, old.master, old.executor)
    slots -= old 
    slots += newSlot
  }

  protected def pullForLaunch(seq: Int, directive: Directive, from: ActorRef) {
    from ! new LaunchTask(directive.task)
    book(seq, directive.task.getId)
  }

  protected def pullForResume(seq: Int, directive: Directive, from: ActorRef) {
    sender ! new ResumeTask(directive.task)
    book(seq, directive.task.getId)
  }

  /**
   * Executor on behalf of Container requests for task execution.
   * - dequeue a directive from queue.
   * - perform function accoding to {@link Directive#action}.
   * - move the directive to pending queue waiting for ack.
   * @return Receive is partial function.
   */
  protected def pullForExecution: Receive = {
    case PullForExecution(slotSeq) => if(!directiveQueue.isEmpty) {
      LOG.info("{} is asking for task execution...", sender.path.name)
      val (slotToDirective, rest) = directiveQueue.dequeue 
      val seq = slotToDirective.seq
      val directive = slotToDirective.directive
      directive.action match {
        case Launch => {
          LOG.info("{} requests for LaunchTask.", sender.path.name)
          pullForLaunch(seq, directive, sender)
        }
        case Kill => LOG.warning("Container {} shouldn't request kill action!",
                                 sender.path.name)
        case Resume => {
          LOG.info("{} requests for ResumeTask.", sender.path.name)
          pullForResume(seq, directive, sender)
        }
        case _ => LOG.warning("Unknown action {} for task {} from master {}", 
                              directive.action, directive.task.getId, 
                              directive.master)
      }
      directiveQueue = rest
    }
  }

  protected def stopExecutor: Receive = {
    case StopExecutor(slotSeq) => slots.find( slot => 
      slot.seq == slotSeq && !None.equals(slot.executor)
    ) match { 
      case Some(found) => found.executor match { 
        case Some(executor) => executor ! StopProcess
        case None => throw new RuntimeException("Impossible! slot "+slotSeq+
                                                "no executor exists!")
      }
      case None => LOG.info("Executor may not be initialized for slot seq {}.",
                            slotSeq)
    }
  }

  protected def containerStopped: Receive = {
    case ContainerStopped => {
      preContainerStopped(sender)
      whenContainerStopped(sender)
      postContainerStopped(sender)
    }
  }
 
  protected def preContainerStopped(executor: ActorRef) {}

  /**
   * When container stopped
   */
  protected def whenContainerStopped(from: ActorRef) = slots.find( slot =>
    slot.executor match { 
      case Some(found) => found.path.name.equals(from.path.name)
      case None => false
    }
  ) match { 
    case Some(found) => found.executor match {
      case Some(executor) => {
        LOG.debug("Send shutdown container message to {} ...", executor)
        executor ! ShutdownContainer
        val taskAttemptId = found.taskAttemptId
        if(!None.equals(taskAttemptId)) 
          LOG.error("Task {} at slot seq {} is not empty! ", taskAttemptId,
                    found.seq)
        val newSlot = Slot(found.seq, None, found.master, None)
        slots -= found
        slots += newSlot
      }
      case None => LOG.error("Executor not found for ", from.path.name)
    }
    case None => LOG.error("No executor found for ", from.path.name)
  }

  protected def postContainerStopped(executor: ActorRef) {}

  protected def messageFromCollector: Receive = { 
    case GetGroomStats => sender ! currentGroomStats
  }

  override def offline(from: ActorRef) = from.path.name match {
    case name if name.contains("_executor_") => from.path.name.split("_") match{
      case ary if (ary.size == 3) => extractSeq(ary(2), { seq => 
        matchSlot(seq, { (seq, id) => 
          matchTaskAttemptId(seq, id, { seq => 
            checkRetries(seq, { seq => newExecutor(seq) })
          }) 
        }) 
      })
      case _ => LOG.error("Invalid executor name", from.path.name)
    }
    case _ => LOG.warning("Unknown actor {} offline!", from.path.name)
  }

  protected def checkRetries(seq: Int, f:(Int) => Unit) = 
    retries.get(seq) match {
      case Some(retryCount) if (retryCount < maxRetries) => {
        f(seq) 
        val v = retryCount + 1
        retries ++= Map(seq -> v)
      }
      case Some(retryCount) if (retryCount >= maxRetries) => { 
        // TODO: update slots executor to None
        //       send report to master as black list 
        //         or remove slot then update groom stats and report master
        //       reset retries map (seq -> 0)
      }
      case None => { // 0
        f(seq)
        retries ++= Map(seq -> 1)
      }
    }

  /**
   * If a task attempt id is found in slot, denoting a task running at that
   * executor fails, task counsellor notify schedule that a task fails; 
   * otherwise respawning executor.
   */
  protected def matchTaskAttemptId(seq: Int, 
                                   taskAttemptId: Option[TaskAttemptID], 
                                   f: (Int) => Unit) = 
    taskAttemptId match {
      case Some(runningTaskId) => groom ! TaskFailure(runningTaskId)
      case None => f(seq) // whether a task in queue or not, restart execturor
    }

  protected def matchSlot(seq: Int, f: (Int, Option[TaskAttemptID]) => Unit) = 
    slots.find( slot => slot.seq == seq) match {
      case Some(found) => f(found.seq, found.taskAttemptId)
      case None => LOG.error("No slot seq found for executor {}", seq)
    }

  protected def extractSeq(seq: String, f:(Int) => Unit) = 
    Try(seq.toInt) match {
      case Success(seq) => f(seq)
      case Failure(cause) => LOG.error("Invalid executor seq {} for {}",
                                       seq, cause)
    }

  override def receive = tickMessage orElse messageFromCollector orElse killAck orElse pullForExecution orElse stopExecutor orElse containerStopped orElse receiveDirective orElse superviseeIsTerminated orElse unknown

}
