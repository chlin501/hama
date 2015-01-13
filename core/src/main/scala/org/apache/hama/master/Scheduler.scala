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
import org.apache.hama.Event
import org.apache.hama.HamaConfiguration
import org.apache.hama.LocalService
import org.apache.hama.Periodically
import org.apache.hama.SubscribeEvent
import org.apache.hama.SystemInfo
import org.apache.hama.Tick
import org.apache.hama.bsp.BSPJobID
import org.apache.hama.bsp.v2.ExceedMaxTaskAllowedException
import org.apache.hama.bsp.v2.Job
import org.apache.hama.bsp.v2.Task
import org.apache.hama.conf.Setting
import org.apache.hama.groom.RequestTask
import org.apache.hama.groom.TaskFailure
import org.apache.hama.master.Directive.Action
import org.apache.hama.master.Directive.Action._
import org.apache.hama.monitor.GroomStats
import org.apache.hama.monitor.master.TaskArrivalEvent
import org.apache.hama.monitor.PublishEvent
import org.apache.hama.monitor.PublishMessage
import scala.collection.immutable.Queue
import scala.collection.JavaConversions._
import scala.util.Failure
import scala.util.Success
import scala.util.Try

sealed trait SchedulerMessage
final case object NextPlease extends SchedulerMessage with Tick
final case class GetTargetRefs(infos: Array[SystemInfo]) 
      extends SchedulerMessage
// TODO: merge TargetRefs and SomeMatched into e.g. TargetRefsFound?
final case class TargetRefs(refs: Array[ActorRef]) extends SchedulerMessage
final case class SomeMatched(matched: Array[ActorRef],
                             nomatched: Array[String]) extends SchedulerMessage
final case class FindGroomsToKillTasks(infos: Set[SystemInfo]) 
      extends SchedulerMessage
final case class GroomsFound(matched: Set[ActorRef], nomatched: Set[String])
      extends SchedulerMessage
final case class TaskCancelled(taskAttemptId: String) extends SchedulerMessage

final case object JobFinishedEvent extends PublishEvent

object JobFinishedMessage {
  def apply(id: BSPJobID): JobFinishedMessage = {
    val msg = new JobFinishedMessage
    msg.id = id 
    msg
  }
}

final class JobFinishedMessage extends PublishMessage {

  protected[master] var id = new BSPJobID()

  override def event(): PublishEvent = JobFinishedEvent

  override def msg(): Any = id

}

object Scheduler {

  def simpleName(conf: HamaConfiguration): String = conf.get(
    "master.scheduler.name",
    classOf[Scheduler].getSimpleName
  )

}

sealed trait Command
final case class KillJob(reason: String) extends Command

// TODO: - separate schedule functions from concrete impl.
//         e.g. class WrappedScheduler(setting: Setting, scheduler: Scheduler)
//         trait scheduluer#assign // passive
//         trait scheduluer#schedule // active
//       - update internal stats to related tracker
//       - job manager dealing with moving job between different states (task 
//         assign, processing, finished, etc.) refactor for better structure.
class Scheduler(setting: Setting, master: ActorRef, receptionist: ActorRef,
                federator: ActorRef) extends LocalService with Periodically {

  type TaskAssignQueue = Queue[Ticket]

  type ProcessingQueue = Queue[Ticket]

  type JobId = String
  
  type IsProcessing = Boolean

  /**
   * A queue that holds a job having tasks unassigned to GroomServers.
   * It should cntain one job only at current implementation .
   * Note: The job in this queue are processed sequentially. Only after a job 
   *       with all tasks are dispatched to GroomServers and is moved to 
   *       processingQueue the next job will be processed. 
   */
  // TODO: change to map (job id -> ticket) ?
  protected var taskAssignQueue = Queue[Ticket]() 

  /**
   * A queue that holds jobs having tasks assigned to GroomServers.
   * A {@link Job} in this queue may be moved back to taskAssignQueue if crash
   * events occurs.
   */
  protected var processingQueue = Queue[Ticket]()

  protected var finishedQueue = Queue[Ticket]()

  protected var command = Map.empty[JobId, Command]

  protected var activeFinished = false 

  // TODO: move the finished job to Federator's JobHistoryTracker, 
  //       where storing job's metadata e.g. setting

  override def initializeServices = {
    master ! SubscribeEvent(GroomLeaveEvent, RequestTaskEvent, TaskFailureEvent)
    federator ! SubscribeEvent(TaskArrivalEvent) 
    LOG.debug("Listening to groom leave, request task, and task failure "+
              "events!")
    tick(self, NextPlease)
  }

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
      activeFinished = false 
      activeSchedule(ticket.job) 
    }
  }

  /**
   * Active schedule tasks within a job to particular GroomServers.
   * Tasks scheduled will be placed in target GroomServer's queue if free slots
   * are not available.
   * @param job contains tasks to be scheduled.
   */
  protected def activeSchedule(job: Job) = job.targetGrooms match {
    case null => 
    case _ => job.targetGrooms.length match { 
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
 
  protected def targetRefsFound(refs: Array[ActorRef]) = 
    if(!taskAssignQueue.isEmpty) {
      val (ticket, rest) = taskAssignQueue.dequeue
      refs.foreach( ref => ticket.job.nextUnassignedTask match {
        case null => moveToProcessingQueue(ticket, rest)
        case task@_ => {
          val (host, port) = getTargetHostPort(ref)
          LOG.debug("Task {} is scheduled to target host {} port {}", 
                   task.getId, host, port)
          task.scheduleTo(host, port)
// TODO: check if its task id is larger than 1. if true change to new Directive(Resume, task, setting.name). might not needed. because active schedule fails leads to reject.
          ref ! new Directive(Launch, task, setting.name)
        }
      })
      activeFinished = true
    }

  /**
   * Master replies after scheduler asks for groom references.
   */
  protected def targetsResult: Receive = { // TODO: some matched to target refs 
    case TargetRefs(refs) => targetRefsFound(refs)
    case SomeMatched(matched, nomatched) => if(!taskAssignQueue.isEmpty) {
      taskAssignQueue.dequeue match { 
        case tuple: (Ticket, Queue[Ticket]) => {
          tuple._1.client ! Reject("Grooms "+nomatched.mkString(", ")+
                                   " do not exist!")
          fromTaskAssignToFinished
        }
        case _ =>
      }
    } else LOG.error("Can't schedule tasks because TaskAssign queue is empty!")
  }

  protected def fromTaskAssignToFinished() = if(!taskAssignQueue.isEmpty) {
    val (ticket, rest) = taskAssignQueue.dequeue
    finishedQueue = finishedQueue.enqueue(ticket)
    whenJobFinished(ticket.job.getId)
    taskAssignQueue = rest 
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
      // TODO: make sure all active tasks are scheduled before passive assign begins; before that, perhaps drop request
      if(activeFinished) passiveAssign(req.stats, sender)
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
// TODO: check if its task attempt id > 1. if true, change to from ! new Directive(Resume, task, setting.name)
          from ! new Directive(Launch, task, setting.name)
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

  protected def ticketWithStage(): (Ticket, IsProcessing) = 
    if(!processingQueue.isEmpty) (processingQueue.head, true) 
    else if(!taskAssignQueue.isEmpty) (taskAssignQueue.head, false) else 
    throw new RuntimeException("TaskAssign and Processing queue are empty!")

  // TODO: reschedule/ reassign tasks
  //       if it's the active target grooms that fail, 
  //         kill tasks, fail job and notify client.
  //       else wait for other groom requesting for task.
  protected def events: Receive = {
    case GroomLeave(name, host, port) => {
      val (ticket, isProcessing) = ticketWithStage
      val job = ticket.job
      val failedTasks = job.findTasksBy(host, port)
      if(0 == failedTasks.size) 
        LOG.debug("No tasks run on failed groom {}:{}!", host, port)
      else failedTasks.exists( task => task.isActive) match {
        case true => { // contain active task, need to reject back to client
          failedTasks.foreach( task => task.failedState)
          val reason = "Active scheduled tasks at %s:%s fail!".
                       format(host, port)
          command = Map(job.getId.toString -> KillJob(reason))
          val groomsAlive = asScalaSet(job.tasksRunAt).toSet.filter( groom => 
            !host.equals(groom.getHost) && (port != groom.getPort)
          )  
          master ! FindGroomsToKillTasks(groomsAlive)
        }
        case false => allPassiveTasks(isProcessing, job, failedTasks) match {
          case Success(result) => 
          case Failure(ex) => ex match {
            case e: ExceedMaxTaskAllowedException => { 
              val reason = "Fail rearranging task: %s!".format(e)
              command = Map(job.getId.toString -> KillJob(reason))
              val grooms = asScalaSet(job.tasksRunAt).toSet.filter( groom => 
                !(host.equals(groom.getHost) && (port == groom.getPort))
              )
              master ! FindGroomsToKillTasks(grooms)  
            }
            case _ => LOG.error("Unexpected exception: {}", ex)
          }
        }
      } 
      // TODO: check if any tasks are assigned to offline groom
      //       if true, check if it's active 
      //         if it's active, fin all sched tasks and kill, then reject  
      //       if it's passive, send kill to groom. when ack is received, 
      //         remove marker.
    } 
    case latest: Task => {
      // TODO: update task in queue.
      //       check if all tasks are successful. if true, call whenJobFinished.
    }
    case fault: TaskFailure => {
      // TODO: reschedule the task by checking task's active groom setting.
      //       - search fault.taskAttemptId in queue's job.
      //       - check if the task is active or passive:
      //         if active, check if target grooms have free slots avail.
      //            if free slots avail, 
      //               a. clone a new task with (id + 1), 
      //               b. update related task data, stats, etc.
      //               c. sched to the free slot 
      //            if no free slots avail, 
      //               a. mark job as failed 
      //               b. reject back to the client
      //               c.  move the job to finished (history?) queue.
      //         if passive
      //            a. clone task with (id + 1)
      //            b. add that task in the job. groom will request for exec
    }
  }

  /**
   * Add a new task to the end of corresponded column in task table.
   * Move job from processing queue back to task assign queue.
   */
  protected def allPassiveTasks(isProcessing: Boolean, job: Job, 
                             fails: java.util.List[Task]): Try[Boolean] = try { 
    fails.foreach { failedTask => job.rearrange(failedTask) } 
    if(isProcessing) {
      val (ticket, rest) = processingQueue.dequeue
      taskAssignQueue = taskAssignQueue.enqueue(ticket)
      processingQueue = rest
    } 
    Success(true)
  } catch {
    case e: Exception => Failure(e) 
  }

  /**
   * Scheduler asks master for grooms references where tasks are running by 
   * issuing FindGroomsToKillTasks.
   * Once receiving grooms references, issue kill command to groom servers.
   */
  protected def groomsFound: Receive = { 
    case GroomsFound(matched, nomatched) => {
      if(!nomatched.isEmpty) 
        LOG.error("Some grooms {} not found!", nomatched.mkString(",")) 
      matched.foreach( ref => {
        val host = ref.path.address.host.getOrElse(null)
        val port = ref.path.address.port.getOrElse(-1)
        val (ticket, isProcessing) = ticketWithStage
        ticket.job.findTasksBy(host, port).foreach ( task => 
          ref ! new Directive(Kill, task, setting.name) 
        )
      })
    }
  }

  protected def updateQueue(ticket: Ticket, isProcessing: Boolean) {
    val job = ticket.job
    val client = ticket.client
    if(isProcessing) processingQueue = processingQueue.updated(0,
                     Ticket(client, job.newWithFailedState))
    else taskAssignQueue = taskAssignQueue.updated(0,
                           Ticket(client, job.newWithFailedState))
  }


  /**
   * In processing queue:
   * - Mark task as cancelled.
   * - Check if all tasks in the job are stopped, either cancelled or failed.
   * If all tasks are marked as stopped:
   *  - Issue job finished event.
   *  - Move job from processing queue to finished queue.
   *  - Notify client.
   */
  protected def taskCancelled: Receive = {   // TODO: need refactor
    case TaskCancelled(taskAttemptId) => {
      val (ticket, isProcessing) = ticketWithStage
      val job = ticket.job
      val client = ticket.client
      job.markAsCancelled(taskAttemptId) match {
        case true => if(job.allTasksStopped) {
          updateQueue(ticket, isProcessing)
          val jobId = ticketWithStage._1.job.getId
          whenJobFinished(jobId) 
          //fromProcessingToFinished  TODO: move(Job)ToFinished
          command.get(jobId.toString) match {  
            case Some(found) if found.isInstanceOf[KillJob] => 
              client ! Reject(found.asInstanceOf[KillJob].reason)  
            case Some(found) if !found.isInstanceOf[KillJob] => 
              LOG.warning("Unknown command {} to react for job {}", found, 
                          jobId)
            case None => 
              LOG.warning("No command in reacting to task cancelled event!")
          }
          command -= jobId.toString 
        }
        case false => LOG.error("Unable to mark task {} killed!", taskAttemptId)
      }
    }
  }

  /**
   * Move the job from processing queue to finished queue.
   */
  protected def fromProcessingToFinished() = if(!processingQueue.isEmpty) {
    val (ticket, rest) = processingQueue.dequeue
    finishedQueue = finishedQueue.enqueue(ticket) // TODO: move to job history?
    whenJobFinished(ticket.job.getId)
    processingQueue = rest 
  }

  /**
   * This function is called when a job if finished its execution.
   */
  protected def whenJobFinished(jobId: BSPJobID) =  
    federator ! JobFinishedMessage(jobId) 

  override def receive = taskCancelled orElse groomsFound orElse tickMessage orElse requestTask orElse dispense orElse targetsResult orElse unknown

}
