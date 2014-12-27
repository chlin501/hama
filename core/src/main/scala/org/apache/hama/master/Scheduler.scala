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
package org.apache.hama.master

import akka.actor.ActorRef
import akka.actor.Cancellable
import org.apache.hama.HamaConfiguration
import org.apache.hama.LocalService
import org.apache.hama.Periodically
import org.apache.hama.SystemInfo
import org.apache.hama.Tick
import org.apache.hama.bsp.v2.Job
import org.apache.hama.bsp.v2.Task
import org.apache.hama.conf.Setting
import org.apache.hama.groom.RequestTask
import org.apache.hama.groom.TaskFailure
import org.apache.hama.master.Directive.Action
import org.apache.hama.master.Directive.Action.Launch
import org.apache.hama.master.Directive.Action.Kill
import org.apache.hama.master.Directive.Action.Resume
import org.apache.hama.monitor.GroomStats
import scala.collection.immutable.Queue

sealed trait SchedulerMessages
final case object NextPlease extends SchedulerMessages with Tick
final case class GetTargetRefs(infos: Array[SystemInfo]) 
      extends SchedulerMessages
final case class TargetRefs(refs: Array[ActorRef]) extends SchedulerMessages
final case class SomeMatched(matched: Array[ActorRef],
                             unmatched: Array[String]) extends SchedulerMessages

object Scheduler {

  def simpleName(conf: HamaConfiguration): String = conf.get(
    "master.scheduler.name",
    classOf[Scheduler].getSimpleName
  )

}

// TODO: - separate schedule functions from this concrete impl.
//         e.g. class WrappedScheduler(setting: Setting, scheduler: Scheduler)
//         trait scheduluer#assign // passive
//         trait scheduluer#schedule // active
//       - update internal stats to related tracker
class Scheduler(setting: Setting, master: ActorRef, receptionist: ActorRef) 
      extends LocalService with Periodically {

  type TaskAssignQueue = Queue[Ticket]
  type ProcessingQueue = Queue[Ticket]

  /**
   * A queue that holds jobs with tasks left unassigning to GroomServers.
   * N.B.: Jobs in this queue are processed sequentially. Only after a job 
   *       with all tasks are dispatched to GroomServers and is moved to 
   *       processingQueue the next job will be processed. 
   */
  protected var taskAssignQueue = Queue[Ticket]()

  /**
   * A queue that holds jobs having tasks assigned to GroomServers.
   * A {@link Job} in this queue may be moved back to taskAssignQueue if crash
   * events occurs.
   */
  protected var processingQueue = Queue[Ticket]()

  // TODO: move the finished job to Federator's JobHistoryTracker, 
  //       where storing job's metadata e.g. setting

  override def initializeServices = tick(self, NextPlease)

  /**
   * Check if task assign queue is empty.
   * @return true if task assign queue is empty; otherwise false.
   */
  protected def isTaskAssignQueueEmpty: Boolean = taskAssignQueue.isEmpty

  /**
   * Check if processing queue is empty.
   * @return true if task processing queue is empty; otherwise false.
   */
  protected def isProcessingQueueEmpty: Boolean = processingQueue.isEmpty

  /**
   * Periodically check if pulling a job for processing is needed.
   * @param message denotes which action to execute.
   */
  override def ticked(message: Tick) = message match {
    case NextPlease => if(isTaskAssignQueueEmpty && isProcessingQueueEmpty)
      receptionist ! TakeFromWaitQueue
    case _ => LOG.warning("Unknown tick message {} for {}", name, message)
  }

  /**
   * Move a job to a specific queue pending for further processing
   *
   * If a job contains particular target GroomServer, schedule tasks to those
   * GroomServers.
   * 
   * Assume GroomServer's maxTasks is not changed over time. (Maybe dynamic 
   * in the future.)
   */
  protected def dispense: Receive = {
    case Dispense(ticket) => { 
      taskAssignQueue = taskAssignQueue.enqueue(ticket)
      activeSchedule(ticket.job) 
    }
  }

  /**
   * Active schedule tasks within a job to particular GroomServers.
   * Tasks scheduled will be placed in target GroomServer's queue if free slots
   * are not available.
   * @param job contains tasks to be scheduled.
   */
  protected def activeSchedule(job: Job) = job.getTargets match {
    case null => 
    case _ => job.getTargets.length match { 
      case 0 =>
      case _ => schedule(job)
    }
  }

  // TODO: allow impl to obtain stats, etc. from tracker   
  protected def schedule(job: Job) {
    val targetGrooms = job.targetInfos  
    LOG.debug("{} requests target grooms refs {} for scheduling!", 
             name, targetGrooms.mkString(","))
    master ! GetTargetRefs(targetGrooms)
  }
 
  protected def targetRefsFound(refs: Array[ActorRef]) {
    val (ticket, rest) = taskAssignQueue.dequeue
    refs.foreach( ref => ticket.job.nextUnassignedTask match {
      case null => moveToProcessingQueue(ticket, rest)
      case task@_ => {
        val (host, port) = getTargetHostPort(ref)
        LOG.debug("Task {} is scheduled to target host {} port {}", 
                 task.getId, host, port)
        task.scheduleTo(host, port)
        ref ! new Directive(Launch, task, setting.hama.get("master.name", 
                                                           setting.name))
      }
    })
  }

  /**
   * Master replies after scheduler asks for groom references.
   */
  protected def targetsResult: Receive = {
    case TargetRefs(refs) => targetRefsFound(refs)
    case SomeMatched(matched, unmatched) => taskAssignQueue.dequeue match { 
      case tuple: (Ticket, Queue[Ticket]) => {
        tuple._1.client ! Reject("Grooms "+unmatched.mkString(", ")+
                                 " are missing!")
      }
      case _ =>
    }
  }

  protected def getTargetHostPort(ref: ActorRef): (String, Int) = {
    val host = ref.path.address.host.getOrElse("")
    val port = ref.path.address.port.getOrElse(50000)
    (host, port)
  }

  /**
   * GroomServer's TaskCounsellor requests for assigning a task.
   * @return Receive partiail function.
   */
  def requestTask: Receive = {
    case req: RequestTask => {
      LOG.debug("GroomServer form {} at {}:{} requests for assigning a task.", 
                sender.path.name, req.stats.map { s => s.host}, 
                req.stats.map { s=> s.port})
      passiveAssign(req.stats, sender)
    }
  } 

  protected def passiveAssign(stats: Option[GroomStats], from: ActorRef) = 
    if(!taskAssignQueue.isEmpty) {
      val (ticket, rest) = taskAssignQueue.dequeue
      stats.map { s => assign(ticket, rest, s, from) }
    }

  protected def assign(ticket: Ticket, rest: TaskAssignQueue, stats: GroomStats,
                       from: ActorRef) {
    val currentTasks = ticket.job.getTaskCountFor(stats.hostPort)
    val maxTasksAllowed = stats.maxTasks
    LOG.debug("Currently there are {} tasks at {}, with max {} tasks allowed.", 
             currentTasks, stats.host, maxTasksAllowed)
    (maxTasksAllowed >= (currentTasks+1)) match {
      case true => ticket.job.nextUnassignedTask match {
        case null => moveToProcessingQueue(ticket, rest)
        case task@_ => {
          val (host, port) = getTargetHostPort(from)
          LOG.debug("Task {} is assigned with target host {} port {}", 
                   task.getId, host, port)
          task.assignedTo(host, port)
          from ! new Directive(Launch, task, setting.hama.get("master.name", 
                                                              setting.name))
        }
      }
      case false => LOG.warning("Drop GroomServer {} requests for a new task "+ 
                                "because the number of tasks exceeds {} "+
                                "allowed!", stats.host, maxTasksAllowed) 
    }
  }

  /**
   * Move ticket to processing queue because all tasks are dispatched. 
   * @param ticket contains job and client reference.
   * @param rest is the queue after dequeuing ticket.
   */
  protected def moveToProcessingQueue(ticket: Ticket, rest: TaskAssignQueue) = 
    if(!taskAssignQueue.isEmpty) {
      taskAssignQueue = rest
      processingQueue = processingQueue.enqueue(ticket) 
    }

  // TODO: reschedule/ reassign tasks
  //       if it's the target grooms that fail 
  //          if other target grooms has free slots, then reschdule
  //          else fail job and notify client.
  //       else wait for other groom requesting for task.

  protected def taskFailure: Receive = {
    case fault: TaskFailure => // TODO: do reschedule task 
  }

  override def receive = taskFailure orElse tickMessage orElse requestTask orElse dispense orElse targetsResult orElse unknown
}
