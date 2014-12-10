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
import org.apache.hama.monitor.GetTaskStats
import org.apache.hama.monitor.GroomStats
import org.apache.hama.monitor.GroomStats._
import scala.collection.immutable.Queue

object TaskCounsellor {

  def simpleName(conf: HamaConfiguration): String = conf.get(
    "groom.taskcounsellor.name",
    classOf[TaskCounsellor].getSimpleName
  )

}

// TODO: create stats class (extends writable) with slot, max task, etc. info
//       once started up, pass spec to reporter, which reports to monitor.
class TaskCounsellor(setting: Setting, groom: ActorRef, reporter: ActorRef) 
      extends Service with Spawnable with Periodically {

  /**
   * The max size of slots can't exceed configured maxTasks.
   */
  protected var slots = Set.empty[Slot]

  /**
   * {@link Directive}s are stored in this queue. 
   */
  protected var directiveQueue = Queue[Directive]()

  /**
   * {@link Directive}s to be acked will be placed in this queue. After acked,
   * directive will be removed.
   */
  protected var pendingQueue = Queue[Directive]()

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

  protected def hasTaskInQueue: Boolean = !directiveQueue.isEmpty

  /**
   * Check if there is slot available. 
   * @return Boolean denotes whether having free slots. Tree if free slots 
   *                 available, false otherwise.
   */
  protected def hasFreeSlots(): Boolean = {
    var isOccupied = true
    var hasFreeSlot = false
    slots.takeWhile(slot => {
      isOccupied = !None.equals(slot.taskAttemptId)
      isOccupied
    })
    if(!isOccupied) hasFreeSlot = true else hasFreeSlot = false
    hasFreeSlot
  }
  
  override def ticked(msg: Tick): Unit = msg match {
    case TaskRequest => if(hasFreeSlots) { 
      if(!hasTaskInQueue) groom ! RequestTask(currentGroomStats)
    }
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
  protected def findTargetToKill(task: Task): Option[ActorRef] = 
    slots.find( slot => slot.taskAttemptId match {
      case Some(taskAttemptId) => taskAttemptId.equals(task.getId)
      case None => false
    }) match {
      case Some(slot) => slot.executor 
      case None => None
    }

  /**
   * 1. Create an Executor actor.
   * 2. Send Fork message so that {@link Executor} will fork a child process.
   * Forked child process will send {@link PullForExecution} message for task 
   * execution.
   */
  protected def initializeExecutor(master: String): Unit = pickUp match {
    case Some(slot) => { 
      LOG.debug("Initialize executor for slot seq {}.", slot.seq)
      val executorName = setting.name + "_executor_" + slot.seq // TODO: move to object Executor { def name(setting, seq): String ... }
      val executor = spawn(executorName, classOf[Executor], setting.hama, self)
      executor ! Fork(slot.seq) 
      val newSlot = Slot(slot.seq, None, master, Some(executor))
      slots -= slot
      slots += newSlot
    }
    case None => LOG.debug("All slots are in use! {}", slots.mkString("\n"))
  }

  /**
   * Pick up a slot that is not in use.
   * @return Option[Slot] contains either a slot or None if no slot available.
   */
  protected def pickUp: Option[Slot] = {
    var free: Slot = null
    var flag = true
    slots.takeWhile( slot => {
      val isEmpty = None.equals(slot.taskAttemptId) //TODO: it is also necessary to check if executor is None!!! Otherwise the slot picked up may be (task == None) but executor is already occupied (executor != None).
      if(isEmpty) {
        free = slot
        flag = false
      } else {
        if(null != free) flag = false
      }
      flag
    })
    if(null == free) None else Some(free)
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

  protected def directiveReceived(directive: Directive) {
    LOG.info("xxxxxxxxxxxxx Receive directive action: "+directive.action+" task: "+
             directive.task.getId.toString+" master: "+directive.master)
    directive.action match {
      case Launch | Resume => {
LOG.info("xxxxxxxxxxxxxx Launch or Resume a task ... ")
        initializeExecutor(directive.master) 
        directiveQueue = directiveQueue.enqueue(directive)
      }
      case Kill => findTargetToKill(directive.task) match {
        case Some(executor) => executor ! new KillTask(directive.task.getId)
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
          val newSlot = Slot(slot.seq, Some(task.getId), slot.master, 
                             Some(executor))
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
   * Executor ack for Launch action.
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
  def doKillAck(action: KillAck) = slots.find( slot => {
    val seqEquals = (slot.seq == action.slotSeq)  
    val idEquals = slot.taskAttemptId match {
      case Some(found) => found.equals(action.taskAttemptId)
      case None => false
    }
    seqEquals && idEquals
  }) match {
    case Some(slot) => {
      val newSlot = Slot(slot.seq, None, slot.master, slot.executor)
      slots -= slot 
      slots += newSlot 
      // TODO: inform reporter!! 
    } 
    case None => LOG.warning("Killed task {} not found for slot seq {}. "+
                             "Slots contains {}", action.taskAttemptId, 
                             action.slotSeq, slots)
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
      directive.task.getId.equals(taskAttemptId)) match {
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
        case Kill => // Kill will be issued when receiveDirective, not here.
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
      case None => throw new RuntimeException("Impossible! Executor not "+
                                              "found for "+from.path.name)
    }
    case None => throw new RuntimeException("No executor found for "+
                                            from.path.name)
  }

  protected def postContainerStopped(executor: ActorRef) {}

  protected def report: Receive = { 
    case GetGroomStats =>  sender ! currentGroomStats
    case GetTaskStats => { // sender ! TaskStats(tasks) 
    }
    case rest@_ => LOG.warning("Unknown metrics request for reporting from {}",
                               sender.path.name)
  }

  override def receive = tickMessage orElse report orElse launchAck orElse resumeAck orElse killAck orElse pullForExecution orElse stopExecutor orElse containerStopped orElse receiveDirective orElse unknown

}
