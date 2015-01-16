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
import org.apache.hama.logging.CommonLog
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
// TODO: merge TargetRefs and SomeMatched into e.g. TargetRefsFound
final case class TargetRefs(refs: Array[ActorRef]) extends SchedulerMessage
final case class SomeMatched(matched: Array[ActorRef],
                             nomatched: Array[String]) extends SchedulerMessage
final case class FindGroomsToKillTasks(infos: Set[SystemInfo]) 
      extends SchedulerMessage
final case class GroomsFound(matched: Set[ActorRef], nomatched: Set[String])
      extends SchedulerMessage
final case class TaskKilled(taskAttemptId: String) extends SchedulerMessage

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

sealed trait Stage 
final case object TaskAssign extends Stage
final case object Processing extends Stage
final case object Finished extends Stage

object JobManager {

  protected val stages = List(TaskAssign, Processing, Finished)

  def apply(): JobManager = new JobManager 

}

protected[master] class JobManager extends CommonLog {

  import JobManager._

  type NextStage = Stage
 
  type JobId = String 

  protected var taskAssignQueue = Queue[Ticket]()  
  
  protected var processingQueue = Queue[Ticket]() 

  protected var finishedQueue = Queue[Ticket]() 

  protected var command = Map.empty[JobId, Command] 

  protected[master] def cacheCommand(jobId: String, cmd: Command) = 
    command += ((jobId, cmd))

  protected[master] def getCommand(jobId: String): Option[Command] = {
    val value = command.get(jobId)
    command -= jobId
    value
  }

  protected[master] def readyForNext(): Boolean = if(isEmpty(TaskAssign) && 
    isEmpty(Processing)) true else false

  protected[master] def isEmpty(stage: Stage): Boolean = stage match {
    case TaskAssign => taskAssignQueue.isEmpty
    case Processing => processingQueue.isEmpty
    case Finished => finishedQueue.isEmpty
  }

  protected[master] def enqueue(ticket: Ticket): Unit = 
    taskAssignQueue = taskAssignQueue.enqueue(ticket)

  protected def enqueue(stage: Stage, ticket: Ticket): Unit = stage match {
    case TaskAssign => enqueue(ticket)
    case Processing => processingQueue = processingQueue.enqueue(ticket)
    case Finished => finishedQueue = finishedQueue.enqueue(ticket)
  }
  
  /**
   * Tickets in Finished stage aren't removed at the moment, so it's not shown
   * if any ticket existed at finished stage.
   */
  protected[master] def ticketAt(): (Option[Stage], Option[Ticket]) = 
    headOf(TaskAssign) match {
      case Some(ticket) => (Option(TaskAssign), Option(ticket))
      case None => headOf(Processing) match {
        case Some(ticket) => (Option(Processing), Option(ticket))
        case None => (None, None)
      }
    }

  protected[master] def headOf(stage: Stage): Option[Ticket] = stage match {
    case TaskAssign => if(!isEmpty(TaskAssign)) Option(taskAssignQueue.head)
      else None
    case Processing => if(!isEmpty(Processing)) Option(processingQueue.head)
      else None
    case Finished => if(!isEmpty(Finished)) Option(finishedQueue.head) else
      None
  }

  protected[master] def findJobById(jobId: BSPJobID): 
    (Option[Stage], Option[Job]) = findJobBy(TaskAssign, jobId) match {
      case Some(job) => (Option(TaskAssign), Option(job))
      case None => findJobBy(Processing, jobId) match {
        case Some(job) => (Option(Processing), Option(job))
        case None => findJobBy(Finished, jobId) match {
          case Some(job) => (Option(Finished), Option(job))
          case None => (None, None)
        }
      }
    }

  protected[master] def findJobBy(stage: Stage, jobId: BSPJobID): 
    Option[Job] = stage match {
    case TaskAssign => if(!isEmpty(TaskAssign)) {
      val job = taskAssignQueue.head.job
      if(job.getId.equals(jobId)) Option(job) else None
    } else None
    case Processing => if(!isEmpty(Processing)) { 
      val job = processingQueue.head.job
      if(job.getId.equals(jobId)) Option(job) else None
    } else None
    case Finished => if(!isEmpty(Finished)) {
      val job = finishedQueue.head.job
      if(job.getId.equals(jobId)) Option(job) else None
    } else None 
  }

  protected def dequeue(stage: Stage): Option[Ticket] = 
    stage match {
      case TaskAssign => if(!isEmpty(TaskAssign)) {
        val (ticket, rest) = taskAssignQueue.dequeue
        taskAssignQueue = rest 
        Option(ticket)
      } else None
      case Processing => if(!isEmpty(Processing)){
        val (ticket, rest) = processingQueue.dequeue
        processingQueue = rest 
        Option(ticket)
      } else None
      case Finished => if(!isEmpty(Finished)) {
        val (ticket, rest) = finishedQueue.dequeue
        finishedQueue = rest 
        Option(ticket)
      } else None
    }

  protected[master] def move(jobId: BSPJobID)(target: Stage): Boolean = 
    findJobById(jobId) match {
      case (s: Some[Stage], j: Some[Job]) => {
        val currentStage = s.get
        val nextStage = stages(stages.indexOf(currentStage) + 1)
        if(nextStage.equals(target)) dequeue(currentStage) match {
          case Some(ticket) => { enqueue(nextStage, ticket) ; true }
          case None => false
        } else false
      }
      case _ => false
    }

  protected[master] def moveToNextStage(jobId: BSPJobID): 
    (Boolean, Option[NextStage]) = findJobById(jobId) match {
      case (stage: Some[Stage], some: Some[Job]) => stage.get match {
        case TaskAssign => {
          dequeue(TaskAssign).map { ticket =>
            processingQueue = processingQueue.enqueue(ticket)
          }
          (true, Option(Processing))
        }
        case Processing => {
          dequeue(Processing).map { ticket => 
            finishedQueue = finishedQueue.enqueue(ticket)
          }
          (true, Option(Finished))
        }
        case Finished => (true, None)
      } 
      case _ => (false, None)
    }
    
  protected[master] def update(newTicket: Ticket): Boolean = 
    findJobById(newTicket.job.getId) match {
      case (s: Some[Stage], j: Some[Job]) => s.get match {
        case TaskAssign => { 
          val q = taskAssignQueue.zipWithIndex.collect { case (ticket, idx) => 
            if(j.get.getId.equals(ticket.job.getId)) idx else -1
          }.filter { p => p != -1 }
          if(1 == q.size && -1 != q.head) {
            taskAssignQueue = taskAssignQueue.updated(q.head, newTicket)
            true
          } else false 
        } 
        case Processing => {
          val q = processingQueue.zipWithIndex.collect { case (ticket, idx) => 
            if(j.get.getId.equals(ticket.job.getId)) idx else -1
          }.filter { p => p != -1 }
          if(1 == q.size && -1 != q.head) {
            processingQueue = processingQueue.updated(q.head, newTicket)
            true
          } else false
        }
        case Finished => { 
          LOG.warning("Updating Job {} at Finished stage!", newTicket.job.getId)
          val q = finishedQueue.zipWithIndex.collect { case (ticket, idx) =>
            if(j.get.getId.equals(ticket.job.getId)) idx else -1
          }.filter { p => p != -1 }
          if(1 == q.size && -1 != q.head) {
            finishedQueue = finishedQueue.updated(q.head, newTicket)
            true
          } else false
        }
      }
      case _ => false
    } 
}

object Scheduler {

  def simpleName(conf: HamaConfiguration): String = conf.get(
    "master.scheduler.name",
    classOf[Scheduler].getSimpleName
  )

}

sealed trait Command

object KillJob {

  def apply(host: String, port: Int): KillJob = 
    KillJob("Active scheduled tasks at %s:%s fail!".format(host, port))

  def apply(cause: Throwable): KillJob =  
    KillJob("Fail rearranging task: %s!".format(cause))

}

final case class KillJob(reason: String) extends Command 

// TODO: - separate schedule functions from concrete impl.
//         e.g. class WrappedScheduler(setting: Setting, scheduler: Scheduler)
//         trait scheduluer#assign // passive
//         trait scheduluer#schedule // active
//       - update internal stats to related tracker
class Scheduler(setting: Setting, master: ActorRef, receptionist: ActorRef,
                federator: ActorRef) extends LocalService with Periodically {

  type JobId = String

  protected val jobManager = JobManager()

  /* a guard for checking if all active tasks are scheduled. */
  protected var activeFinished = false 

  // TODO: move finished jobs to Federator's JobHistoryTracker(?)
  //       store job's metadata e.g. setting, state, etc. only

  override def initializeServices = {
    master ! SubscribeEvent(GroomLeaveEvent, RequestTaskEvent, TaskFailureEvent)
    federator ! SubscribeEvent(TaskArrivalEvent) 
    LOG.debug("Listening to groom leave, request task, and task failure "+
              "events!")
    tick(self, NextPlease)
  }

  /**
   * Periodically check if pulling a job for processing is needed.
   * @param message denotes which action to execute.
   */
  override def ticked(message: Tick) = message match { 
    case NextPlease => if(jobManager.readyForNext)
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
      jobManager.enqueue(ticket) 
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
 
  /**
   * In TaskAssign stage, target groom references found. Next step is to 
   * send the directive to target groom servers.
   * Note that activeFinished flag must be marked as true once all directives
   * are sent to grooms.
   */
  protected def targetRefsFound(refs: Array[ActorRef]) = 
    if(!jobManager.isEmpty(TaskAssign)) {
      jobManager.headOf(TaskAssign).map { ticket => {
        val job = ticket.job
        val expected = job.targetInfos.size
        val actual = refs.size
        if(expected != actual)
          throw new RuntimeException(expected+" target grooms expected, but "+
                                     "only "+actual+" found!")
        refs.foreach( ref => job.nextUnassignedTask match { 
          case null => jobManager.moveToNextStage(job.getId) match {
            case (true, _) => jobManager.update(ticket.
              newWithJob(job.newWithRunningState))
            case _ => LOG.error("Unable to move job {} to next stage!", 
                                job.getId)
          }
          case task@_ => {
            val (host, port) = targetHostPort(ref)
            LOG.debug("Task {} is scheduled to target host {} port {}", 
                      task.getId, host, port)
            task.scheduleTo(host, port)
            if(1 < task.getId.getId) 
              ref ! new Directive(Resume, task, setting.name) 
            else ref ! new Directive(Launch, task, setting.name)
          }
        })
      }}
      activeFinished = true
    }

  /**
   * During TaskAssign Stage, master replies scheduler's request for groom 
   * references.
   */
  // TODO: merge some matched to target refs 
  protected def targetsResult: Receive = { 
    case TargetRefs(refs) => targetRefsFound(refs)
    case SomeMatched(matched, nomatched) => if(!jobManager.isEmpty(TaskAssign))
      jobManager.headOf(TaskAssign).map { ticket => {
        ticket.client ! Reject("Grooms "+nomatched.mkString(", ")+
                               " do not exist!")
        jobManager.move(ticket.job.getId)(Finished) 
      }}
    else LOG.error("Can't schedule because TaskAssign queue is empty!")
  }

  protected def targetHostPort(ref: ActorRef): (String, Int) = {
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
      if(activeFinished) passiveAssign(req.stats, sender)
    }
  } 

  protected def passiveAssign(stats: Option[GroomStats], from: ActorRef) = 
    if(!jobManager.isEmpty(TaskAssign)) {
      jobManager.headOf(TaskAssign).map { ticket => stats.map { s => 
        assign(ticket, s, from) 
      }}
    } else LOG.debug("No job stays at task assign stage!")

  protected def assign(ticket: Ticket, stats: GroomStats, from: ActorRef) {
    val job = ticket.job
    val currentTasks = job.getTaskCountFor(stats.hostPort)
    val maxTasksAllowed = stats.maxTasks
    LOG.debug("Currently there are {} tasks at {}, with max {} tasks allowed.", 
             currentTasks, stats.host, maxTasksAllowed)
    (maxTasksAllowed >= (currentTasks+1)) match {
      case true => job.nextUnassignedTask match {
        case null => jobManager.moveToNextStage(job.getId) match {
          case (true, _) => if(jobManager.update(ticket.newWithJob(
            job.newWithRunningState))) LOG.debug("Job is running!", job.getId) 
          case _ => LOG.error("Unable to move job {} to next stage!", job.getId)
        }
        case task@_ => {
          val (host, port) = targetHostPort(from)
          LOG.debug("Task {} is assigned with target host {} port {}", 
                   task.getId, host, port)
          task.assignedTo(host, port)
          if(1 < task.getId.getId)  
            from ! new Directive(Resume, task, setting.name) 
          else from ! new Directive(Launch, task, setting.name)
        }
      }
      case false => LOG.warning("Drop GroomServer {} requests for a new task "+ 
                                "because the number of tasks exceeds {} "+
                                "allowed!", stats.host, maxTasksAllowed) 
    }
  }

  /**
   * The groom leave event happens, and some tasks run on the failed groom.
   */
  protected def whenActiveTasksFail(host: String, port: Int, job: Job,
                                    failedTasks: java.util.List[Task]) {
    failedTasks.foreach( task => task.failedState)
    jobManager.cacheCommand(job.getId.toString, KillJob(host, port))
    val groomsAlive = asScalaSet(job.tasksRunAt).toSet.filter( groom => 
      !host.equals(groom.getHost) && (port != groom.getPort)
    )  
    master ! FindGroomsToKillTasks(groomsAlive)
  } 

  protected def taskExceedsMaxAttempt(host: String, port: Int, job: Job, 
                                      e: Throwable) {
    jobManager.cacheCommand(job.getId.toString, KillJob(e))
    val grooms = asScalaSet(job.tasksRunAt).toSet.filter( groom => 
      !(host.equals(groom.getHost) && (port == groom.getPort))
    )
    master ! FindGroomsToKillTasks(grooms) 
  }

  // TODO: reschedule/ reassign tasks
  //       if it's the active target grooms that fail, 
  //         kill tasks, fail job and notify client.
  //       else wait for other groom requesting for task.
  protected def events: Receive = {
    case GroomLeave(name, host, port) => {
      // TODO: 
      //       if another groom leaves, check if any tasks runs on that groom.
      //         if true (some tasks run on that groom), check job if in recovering (denoting kill msg is issued)
      //           if true (in recovering), directly mark tasks as failure (for no ack will be received), and check if all tasks stopped.
      //           else check if any tasks is active or not, then route as below

      jobManager.ticketAt match {
        case (s: Some[Stage], t: Some[Ticket]) => {
          // TODO: check job's state if in recovering
          //       if true, skip; otherwise, go as below.
          val job = t.get.job
          val failedTasks = job.findTasksBy(host, port)
          failedTasks.size match {
            case 0 => LOG.debug("No tasks on failed groom {}:{}!", host, port)
            case _ => {
              val recovering = job.newWithRecoveringState
              jobManager.update(t.get.newWithJob(recovering)) 
              failedTasks.exists(task => task.isActive) match { 
                case true => whenActiveTasksFail(host, port, recovering, 
                                                 failedTasks)
                case false => allPassiveTasks(s.get, recovering, 
                                              failedTasks) match {
                  case Success(result) => 
                  case Failure(ex) => ex match {
                    case e: ExceedMaxTaskAllowedException => 
                      taskExceedsMaxAttempt(host, port, recovering, ex)
                    case _ => LOG.error("Unexpected exception: {}", ex)
                  }
                }
              }
            }
          }
        }
        case _ => LOG.warning("No job existed!")
      }
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
      //               c. issue cancel action via directive 
      //               d. when receiving task cancelled msg (in Receive func)
      //                  check if all tasks stopped, if true:
      //                    - move job to task assign stage and
      //                    - call activeSchedule functions
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
  protected def allPassiveTasks(stage: Stage, job: Job, 
                                fails: java.util.List[Task]): Try[Boolean] = 
    try { 
      fails.foreach { failedTask => job.rearrange(failedTask) } 
      stage match { 
        case Processing => jobManager.move(job.getId)(TaskAssign)
        case _ => throw new RuntimeException("Job "+job.getId+" is not at "+
                                             "Processing stage!")
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
        jobManager.ticketAt match {
          case (s: Some[Stage], t: Some[Ticket]) => 
            t.get.job.findTasksBy(host, port).foreach ( task => 
              ref ! new Directive(Kill, task, setting.name) 
            )
          case _ => 
        }
      })
    }
  }

  /**
   * This happens when the groom leave event is triggered and where 
   * FindGroomsToKillTask is issued with active tasks found or task exceedng 
   * max attempt upper bound.
   * 
   * In processing queue:
   * - Mark task as cancelled.
   * - Check if all tasks in the job are stopped, either cancelled or failed.
   * If all tasks are marked as stopped:
   *  - Issue job finished event.
   *  - Move job from processing queue to finished queue.
   *  - Notify client.
   */
  protected def taskCancelled: Receive = {   // TODO: need refactor
    case TaskKilled(taskAttemptId) => jobManager.ticketAt match {
      case (stage: Some[Stage], t: Some[Ticket]) => { 
        val job = t.get.job
        val client = t.get.client
        job.markAsCancelled(taskAttemptId) match {
          case true => if(job.allTasksStopped) {
            // Note: newjob is not in cancelled state because it's a groom leave
            //       event.
            val newJob = job.newWithFailedState 
            jobManager.update(t.get.newWithJob(newJob))
            val jobId = newJob.getId
            whenJobFinished(jobId) 
            jobManager.move(jobId)(Finished)
            jobManager.getCommand(jobId.toString) match {
              case Some(found) if found.isInstanceOf[KillJob] => 
                client ! Reject(found.asInstanceOf[KillJob].reason)  
              case Some(found) if !found.isInstanceOf[KillJob] => 
                LOG.warning("Unknown command {} to react for job {}", found, 
                            jobId)
              case None => LOG.warning("No matched command for job {} in "+
                                       "reacting to cancelled event!", jobId)
            }
          }
          case false => LOG.error("Unable to mark task {} killed!", 
                                  taskAttemptId)
        }
      }
      case _ => LOG.error("No ticket found at any Stage!")
    }
  }

  /**
   * This function is called when a job if finished its execution.
   */
  protected def whenJobFinished(jobId: BSPJobID) =  
    federator ! JobFinishedMessage(jobId) 

  override def receive = taskCancelled orElse groomsFound orElse tickMessage orElse requestTask orElse dispense orElse targetsResult orElse unknown

}
