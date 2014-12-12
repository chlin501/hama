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

object TaskFailure {

  def apply(id: TaskAttemptID): TaskFailure = {
    val taskFailure = new TaskFailure()
    taskFailure.id = id
    taskFailure
  }
}

final class TaskFailure extends Writable {

  protected[groom] var id: TaskAttemptID = new TaskAttemptID

  def taskAttemptId(): TaskAttemptID = id

  @throws(classOf[IOException])
  override def write(out: DataOutput) = id.write(out) 

  @throws(classOf[IOException])
  override def readFields(in: DataInput) {
    id = new TaskAttemptID()
    id.readFields(in)
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

  /**
   * The max size of slots can't exceed configured maxTasks.
   */
  protected var slots = Set.empty[Slot]

  /**
   * {@link Directive}s are stored in this queue when executors are not yet
   * initialized. Once executors are all initializsed, directives will be
   * dispatched to executor directly.
   */
  protected var directiveQueue = Queue.empty[Directive]

  /**
   * {@link Directive}s to be acked will be placed in this queue. After acked,
   * the directive will be removed.
   */
  protected var pendingQueue = Queue.empty[Directive]

  protected def maxTasks(): Int = setting.hama.getInt("bsp.tasks.maximum", 3)

  /**
   * Initialize slots with default slots value to 3, which comes from maxTasks,
   * or "bsp.tasks.maximum".
   * @param constraint of the slots can be created.
   */
  protected def initializeSlots(constraint: Int = 3) {
    for(seq <- 1 to constraint) {
      slots ++= Set(Slot(seq, None, "", None))
    }
    LOG.debug("{} GroomServer slots are initialied.", constraint)
  }


  override def initializeServices = {
    tick(self, TaskRequest)
    initializeSlots(maxTasks)
  }

  /**
   * Check if there is slot available. 
   * @return Boolean denotes whether having free slots. Tree if free slots 
   *                 available, false otherwise.
   */
  protected def hasFreeSlot(): Boolean = {
    var isOccupied = true
    var freeSlot = false
    slots.takeWhile( slot => {
      isOccupied = !None.equals(slot.taskAttemptId)
      isOccupied
    })
    if(!isOccupied) freeSlot = true else freeSlot = false
    freeSlot
  }
  
  /**
   * Periodically request master for a new task when the groom has free slots
   * and no task in directive queue.
   */
  override def ticked(msg: Tick): Unit = msg match {
    case TaskRequest => if(hasFreeSlot) groom ! RequestTask(currentGroomStats)
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
    val queueIds = list(directiveQueue) 
    val slotIds = list(slots) 
    LOG.debug("Current groom stats: name {}, host {}, port {}, maxTasks {}, "+
              "queue tasks ids {}, slots ids {}", name, host, port, maxTasks, 
              queueIds, slotIds)
    GroomStats(name, host, port, maxTasks, queueIds, slotIds)
  } 

  //protected def currentTasks(): TaskStats = {
    
  //}

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
   * 
   * Once initialized or dispatched, corresponded container will acks for 
   * booking task attempt id field in slot.
   */
  protected def initializeOrDispatch(d: Directive) = shouldNewExecutor match {
    case true => slots.find( slot => None.equals(slot.executor)) match {
      case Some(found) => {
        val newSlot = Slot(found.seq, None, d.master, 
                           Option(newExecutor(found)))
        slots -= found
        slots += newSlot
        directiveQueue = directiveQueue.enqueue(d)
      }
      case None =>
    }
    case false => slots.find( slot => None.equals(slot.taskAttemptId)) match {
      case Some(found) => d.action match {
        case Launch => found.executor.map { e => 
          e ! new LaunchTask(d.task) 
          pendingQueue = pendingQueue.enqueue(d)
        }
        case Resume => found.executor.map { e => 
          e ! new ResumeTask(d.task) 
          pendingQueue = pendingQueue.enqueue(d)
        }
        case Kill => // won't be here
      }
      case None =>
    }
  }

  // TODO: when a task finishes, by exaiming stats reported from executor, 
  //       update slot taskAttemptId to None

  protected def shouldNewExecutor(): Boolean = slots.exists( slot =>
    None.equals(slot.executor)
  )

  protected def newExecutor(foundSlot: Slot): ActorRef = { 
    LOG.debug("Initialize executor for slot seq {}.", foundSlot.seq)
    val executorName = Executor.simpleName(setting.hama, foundSlot.seq)
    val executor = spawn(executorName, classOf[Executor], setting.hama, self) 
    executor ! Fork(foundSlot.seq) 
    executor
  }

  /**
   * Receive {@link Directive} from Scheduler, deciding what to do next.
   * @return Receive is partial function.
   */
  protected def receiveDirective: Receive = {
    case directive: Directive => directive match {
      case null => LOG.warning("Directive dispatched from {} is null!", 
                               sender.path.name)
      case _ => directiveReceived(directive) 
    }
  }

  /**
   * Initialize child process if no executor found; otherwise dispatch action 
   * to child process directly.
   * When directive is kill, slot update will be done after ack is received.
   */
  protected def directiveReceived(directive: Directive) {
    LOG.info("Receive directive action: "+directive.action+" task: "+
             directive.task.getId.toString+" master: "+directive.master)
    directive.action match {
      case Launch | Resume => initializeOrDispatch(directive) 
      case Kill => findTarget(directive.task) match {
        case Some(slot) => slot.executor.map { e => 
          e ! new KillTask(directive.task.getId) 
        }
        case None => LOG.warning("Ask to Kill task {}, but no "+
                                 "corresponded executor found!", 
                                 directive.task.toString)
      }
      case d@_ => LOG.warning("Unknown directive {}", d)
    }
  }

  /**
   * Book the slot with corresponded {@link Task} and {@link Executor} when
   * receiving LaunchAck and ResumeAck.
   * @param slotSeq indicates the <i>N</i>th slot.
   * @param task is the task being executed
   * @param executor is the executor that runs the task.
   */
  protected def book(slotSeq: Int, task: Task, executor: ActorRef) {
    slots.find(slot => (slotSeq == slot.seq)) match {
      case Some(slot) => slot.taskAttemptId match {
        case None => {
          val newSlot = Slot(slot.seq, Option(task.getId), slot.master, 
                             Option(executor))
          slots -= slot 
          slots += newSlot
        }
        case Some(found) => 
          throw new RuntimeException("Task "+task.getId+" can't run on slot "+
                                     slotSeq+" because "+found+" is running.")
      }
      case None => 
        throw new RuntimeException("Slot with seq "+slotSeq+" not found "+
                                   "for task "+task.getId+"!")
    }
  }

  /**
   * Executor confirms lauch task received.
   * @return Receive is partial function.
   */
  protected def launchAck: Receive = {
    case action: LaunchAck => {
      preLaunchAck(action)
      doAck(action.slotSeq, action.taskAttemptId, sender)
      postLaunchAck(action)
    }
  }

  protected def preLaunchAck(ack: LaunchAck) { }

  protected def postLaunchAck(ack: LaunchAck) { }

  /**
   * Executor ack for Resume action.
   * @return Receive is partial function.
   */
  protected def resumeAck: Receive = {
    case action: ResumeAck => {
      preResumeAck(action)
      doAck(action.slotSeq, action.taskAttemptId, sender)
      postResumeAck(action)
    }
  }

  protected def preResumeAck(ack: ResumeAck) { }

  protected def postResumeAck(ack: ResumeAck) { }

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

  /**
   * Update slot information according to {@link Slot#seq} and 
   * {@link TaskAttemptID} acked by {@link Executor}.
   * This function:
   * - search directive in pendingQueue.
   * - update slot information with found directive content. 
   * @param slotSeq is the sequence number of slot.
   * @param taskAttemptId is the task attempt id executed at Container.
   */
  protected def doAck(slotSeq: Int, taskAttemptId: TaskAttemptID, 
                      from: ActorRef): Unit = pendingQueue.isEmpty match {
    case true => LOG.warning("Pending queue is empty when slot {}, "+
                             "task {} ack!", slotSeq, taskAttemptId)
    case false => pendingQueue.find( directive => 
      directive.task.getId.equals(taskAttemptId)
    ) match {
      case Some(directive) => { 
        LOG.debug("doAck action: {} task: {} executor: {}", 
                  directive.action, directive.task.getId, from)
        book(slotSeq, directive.task, from)
        pendingQueue = pendingQueue diff Queue(directive)
        // TODO: inform reporter!!
      }
      case None => LOG.error("No pending directive for task {}, slot {} "+
                             "matches ack.", taskAttemptId, slotSeq)
    }
  } 

  /**
   * Executor on behalf of Container requests for task execution.
   * - dequeue a directive from queue.
   * - perform function accoding to {@link Directive#action}.
   * @return Receive is partial function.
   */
  protected def pullForExecution: Receive = {
    case PullForExecution(slotSeq) => if(!directiveQueue.isEmpty) {
      val (directive, rest) = directiveQueue.dequeue 
      directive.action match {
        case Launch => {
          LOG.debug("{} requests for LaunchTask.", sender)
          sender ! new LaunchTask(directive.task)
          pendingQueue = pendingQueue.enqueue(directive)
          directiveQueue = rest 
        }
        case Kill => LOG.warning("Container {} shouldn't request kill action!",
                                 sender.path.name)
        case Resume => {
          LOG.debug("{} requests for ResumeTask.", sender)
          sender ! new ResumeTask(directive.task)
          pendingQueue = pendingQueue.enqueue(directive)
          directiveQueue = rest  
        }
        case _ => {
          LOG.warning("Unknown action {} for task {} from master {}", 
                      directive.action, directive.task.getId, 
                      directive.master)
          directiveQueue = rest
        }
      }
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
          throw new RuntimeException("Task at slot seq "+found.seq+
                                     " is not"+ "empty! task: "+taskAttemptId)
        val newSlot = Slot(found.seq, taskAttemptId, found.master, None)
        slots -= found
        slots += newSlot
      }
      case None => throw new RuntimeException("Executor not found for "+
                                              from.path.name)
    }
    case None => throw new RuntimeException("No executor found for "+
                                            from.path.name)
  }

  protected def postContainerStopped(executor: ActorRef) {}

  protected def messageFromCollector: Receive = { 
    case GetGroomStats =>  sender ! currentGroomStats
    case rest@_ => LOG.warning("Unknown stats request for reporting from {}",
                               sender.path.name)
  }

  /**
   * Delegate to groom notifying master that a task fails.
   * The message comes from container or executor.
   */
  protected def messageFromExecutor: Receive = {
    case ContainerOffline(slotSeq) => slots.find( slot => 
      slot.seq == slotSeq
    ) match {
      case Some(found) => found.taskAttemptId.map { id => 
        groom ! TaskFailure(id) 
        // TODO: also report to tracker for blacklist stats
        //       groom ! ContainerFailure(slotSeq)
        //       then groom forward ContainerFailure(slotSeq) to master
        //       where tracker records groom host:port -> (slot, crash count)
      } 
      case None => LOG.warning("Unknown container with slot seq {} fails!", 
                               slotSeq)
    }
    case IllegalSlotSequence(seq) => 
      LOG.warning("Unable to fork child process due to invalid slot seq "+seq) 
    case PeerProcessCreationFailure(seq, cause) => {
      // TODO: 1. fork again 
      //          record retry times
      //          once reach max retry do cleanup slots info and then go to 2.
      //       2. or report to master in switching task to other node.
      //          groom froward MoveTask(taskAttemptId) to master
      //       
      //       for 1 and 2 both do cleanup slots info associated slot to task
      LOG.error("Fail forking a child process! Reason: {}", cause) 
    }
  }

  override def receive = tickMessage orElse messageFromCollector orElse launchAck orElse resumeAck orElse killAck orElse pullForExecution orElse stopExecutor orElse containerStopped orElse receiveDirective orElse messageFromExecutor orElse unknown

}
