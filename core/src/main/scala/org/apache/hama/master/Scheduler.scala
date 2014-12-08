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
import org.apache.hama.bsp.v2.Job
import org.apache.hama.bsp.v2.Task
import org.apache.hama.groom.RequestTask
import org.apache.hama.conf.Setting
import org.apache.hama.master.Directive.Action
import org.apache.hama.master.Directive.Action.Launch
import org.apache.hama.master.Directive.Action.Kill
import org.apache.hama.master.Directive.Action.Resume
import org.apache.hama.monitor.GroomStats
import org.apache.hama.RemoteService
import scala.collection.immutable.Queue

sealed trait SchedulerMessages
final case object NextPlease extends SchedulerMessages  
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
//       - complete one job at a time
//       - update internal stats to related tracker
class Scheduler(setting: Setting, master: ActorRef, receptionist: ActorRef) 
      extends LocalService with RemoteService with Periodically {

  type TaskAssignQueue = Queue[Ticket]

/*
  type TaskCounsellorRef = ActorRef
  type MaxTasksAllowed = Int
  type ProcessingQueue = Queue[Ticket]
*/

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

  /* Store jobs that finishes its computation. */
  // TODO: move finished jobs to Federator's JobHistoryTracker, where storing job's metadata e.g. setting
  // protected var finishedQueue = Queue[Ticket]() 

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
  override def ticked(message: Any) = message match {
    case NextPlease => if(isTaskAssignQueueEmpty && isProcessingQueueEmpty)
      receptionist ! TakeFromWaitQueue
    case _ => LOG.warning("Unknown tick message {} for {}", name, message)
  }

  /**
   * Check if the taskQueue is empty. If true, ask Receptionist to dispense a 
   * job; otherwise do nothing.
  def nextPlease: Receive = {
    case NextPlease => {
      //if(isTaskAssignQueueEmpty) receptionist ! TakeFromWaitQueue 
    }
  }
   */

  /**
   * Move a job to a specific queue pending for further processing
   *
   * If a job contains particular target GroomServer, schedule tasks to those
   * GroomServers.
   * 
   * Assume GroomServer's maxTasks is not changed over time. (Maybe dynamic 
   * in the future.)
   */
  def dispense: Receive = {
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
  def activeSchedule(job: Job) = job.getTargets match {
    case null => 
    case _ => job.getTargets.length match { 
      case 0 =>
      case _ => schedule(job)
/*
       {
        val (from, to) = schedule(taskAssignQueue)  
        this.taskAssignQueue = from
        to.isEmpty match {
          case false => processingQueue = processingQueue.enqueue(to.dequeue._1)
          case true =>
        }
        LOG.debug("In activeSchedule, taskAssignQueue has {} jobs, and "+
                  "processingQueue has {} jobs", 
                  taskAssignQueue.size, processingQueue.size)
      }
*/
    }
  }

  // TODO: alllow impl to obtain stats from tracker   
  protected def schedule(job: Job) {
    val targetGrooms = job.targetInfos  
    LOG.info("Request target grooms {} refs for scheduling!", 
             targetGrooms.mkString(","))
    master ! GetTargetRefs(targetGrooms)
  }

  /**
   * Master replies after scheduler asks for groom references.
   */
  protected def targetsResult: Receive = {
    case TargetRefs(refs) => {
      val (ticket, rest) = taskAssignQueue.dequeue
      refs.foreach( ref => ticket.job.nextUnassignedTask match {
        case null => moveToProcessingQueue(ticket, rest)
        case task@_ => {
          task.scheduleTo(ref.path.address.host.getOrElse(""),
                          ref.path.address.port.getOrElse(50000))
          ref ! new Directive(Launch, task, setting.hama.get("master.name", 
                                                             setting.name))
        }
      }) 
    }
    case SomeMatched(matched, unmatched) => taskAssignQueue.dequeue match { 
      case tuple: (Ticket, Queue[Ticket]) => {
        tuple._1.client ! Reject("Grooms "+unmatched.mkString(", ")+
                                 " are missing!")
      }
      case _ =>
    }
  }

  /**
   * Positive schedule tasks.
   * Actual function that exhaustively schedules tasks to target GroomServers.
  def schedule(fromQueue: TaskAssignQueue): 
      (TaskAssignQueue, ProcessingQueue) = { 
    LOG.info("TaskAssignQueue size is {}", fromQueue.size)
    val (job, rest) = fromQueue.dequeue
    val targetGrooms = job.targetInfos  
    var from = Queue[Job](); var to = Queue[Job]()
    targetGrooms.foreach( info => { 
      // TODO: ask master for groom actor ref
      //       wait for target groom refs (via msg) 
      //       schedule tasks

        val currentTaskScheduled = job.getTaskCountFor(groomName)
        if(maxTasksAllowed < currentTaskScheduled)
          throw new IllegalStateException("Current tasks "+currentTaskScheduled+
                                          " for "+groomName+" exceeds "+
                                          maxTasksAllowed)
        if((currentTaskScheduled+1) <= maxTasksAllowed)  
          to = bookThenDispatch(job, taskCounsellorActor, groomName, dispatch)
        else throw new RuntimeException("Can't assign task because currently "+
                                        currentTaskScheduled+" tasks scheduled"+
                                        " to groom server "+groomName+", "+
                                        "which allows "+maxTasksAllowed+
                                        " tasks to run.")
    })
    if(!to.isEmpty) from = rest else from = from.enqueue(job)
    LOG.debug("In schedule function, from queue: {} to queue: {}", from, to)
    (from, to)
  }
   */

  /**
   * Mark the task with the corresponded {@link GroomServer}; then dispatch 
   * the task to that {@link GroomServer}.
   * Also if all tasks are assigned, cleanup the task assign queue.
   * @param job contains tasks to be scheduled.
   * @param targetActor is the remote GroomServer's {@link TaskCounsellor}.
   * @param targetGroomServer to which the task will be scheduled.
   * @param d is the dispatch function.
  def bookThenDispatch(job: Job, targetActor: TaskCounsellorRef,  
                       targetGroomServer: String, 
                       d: (TaskCounsellorRef, Action, Task) => Unit): 
      ProcessingQueue = {
    var to = Queue[Job]()
    unassignedTask(job) match {
      case Some(task) => {
        // scan job's tasks checking if sumup of scheduled to the same groom 
        // server's tasks > maxTasks if passive assigned.
        task.markWithTarget(targetGroomServer) 
        d(targetActor, Launch, task)
      }
      case None => 
    }
    LOG.info("Are all tasks assigned? {}", job.areAllTasksAssigned)
    if(job.areAllTasksAssigned) to = to.enqueue(job)
    to
  }
   */

  /** 
   * Dispatch a Task to a GroomServer.
   * @param from is the GroomServer task manager.
   * @param action denotes what action will be performed upon the task.
   * @param task is the task to be executed.
  protected def dispatch(from: TaskCounsellorRef, action: Action, task: Task) {
    from ! new Directive(action, task,  
                         conf.get("master.name", "bspmaster"))  
  }
   */

  /**
   * Dispense next unassigned task. None indiecates all tasks are assigned.
  def unassignedTask(job: Job): Option[Task] = {
    val task = job.nextUnassignedTask;
    if(null != task) Some(task) else None
  }
   */

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
    LOG.info("Currently {} tasks at {}, and {} tasks allowed.", 
             currentTasks, stats.host, maxTasksAllowed)
    (maxTasksAllowed >= (currentTasks+1)) match {
      case true => ticket.job.nextUnassignedTask match {
        case null => moveToProcessingQueue(ticket, rest)
        case task@_ => {
          task.assignedTo(from.path.address.host.getOrElse(""),
                          from.path.address.port.getOrElse(50000))
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
   * Assign a task to the requesting GroomServer's task manager.
   * Assign function follows after schedule one, it means th rest unassigned
   * tasks are all for passive.
   * @param groomServerStat is the most recent stat of a GroomServer.
   * @param taskCounsellor refers to the remote GroomServer TaskCounsellor instance.
  def passiveAssign(stat: GroomServerStat, taskCounsellor: ActorRef) {
      val (from, to) = 
        assign(stat, taskAssignQueue, taskCounsellor, dispatch) 
      this.taskAssignQueue = from
      if(!to.isEmpty) 
        this.processingQueue = processingQueue.enqueue(to.dequeue._1)
      LOG.info("In passiveAssign, taskAssignQueue has {} jobs, and "+
               "processingQueue has {} jobs", 
               taskAssignQueue.size, processingQueue.size)
  }
   */

  /**
   * Assign a task to a particular GroomServer by delegating that task to 
   * dispatch function, which normally uses actor ! message.
  def assign(stat: GroomServerStat, fromQueue: TaskAssignQueue, 
             taskCounsellor: ActorRef, d: (ActorRef, Action, Task) => Unit): 
      (TaskAssignQueue, ProcessingQueue) = {
    if(!fromQueue.isEmpty) {
      val (job, rest) = fromQueue.dequeue 
      var from = Queue[Job]()
      var to = Queue[Job]()
      val currentTaskAssigned = job.getTaskCountFor(stat.getName)
      if(stat.getMaxTasks < currentTaskAssigned)
        throw new IllegalStateException("Current tasks "+currentTaskAssigned+
                                        " for "+stat.getName+" exceeds "+
                                        stat.getMaxTasks+" allowed.")
      if((currentTaskAssigned+1) <= stat.getMaxTasks) 
        to = bookThenDispatch(job, taskCounsellor, stat.getName, d) 
      else 
        LOG.warning("Drop GroomServer {} task request for only {} slots are "+
                    "available and are full.", 
                    stat.getName, stat.getMaxTasks)
      if(!to.isEmpty) from = rest else from = from.enqueue(job)
      LOG.debug("In assign function, from queue: {}, to queue: {}", from, to)
      (from, to)
    } else {
      (fromQueue, Queue[Job]())
    }
  }
   */
  
/*
  * Rescheduling tasks when a GroomServer goes offline.
  def reschedTasks: Receive = {
    case RescheduleTasks(spec) => {
       LOG.info("Failed GroomServer having GroomServerSpec "+spec)
       // TODO: 1. check if job.getTargets is empty or not.
       //       2. if targets has values, call schedule(); otherwise 
       //          put into assign task queue, waiting for groom request.
    }
  }
*/

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

  override def receive = tickMessage orElse /*requestTask orElse*/ dispense /*orElse nextPlease*/ orElse targetsResult orElse timeout orElse unknown
}
