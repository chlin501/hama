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
import org.apache.hama.bsp.TaskAttemptID
import org.apache.hama.bsp.v2.TaskMaxAttemptedException
import org.apache.hama.bsp.v2.Job
import org.apache.hama.bsp.v2.Job.State._
import org.apache.hama.bsp.v2.Task
import org.apache.hama.conf.Setting
import org.apache.hama.client.JobComplete
import org.apache.hama.groom.NoFreeSlot
import org.apache.hama.groom.RequestTask
import org.apache.hama.groom.TaskFailure
import org.apache.hama.logging.CommonLog
import org.apache.hama.master.Directive.Action
import org.apache.hama.master.Directive.Action._
import org.apache.hama.monitor.GroomStats
import org.apache.hama.monitor.master.GetGroomCapacity
import org.apache.hama.monitor.master.GroomCapacity
import org.apache.hama.monitor.master.GroomsTracker
import org.apache.hama.monitor.master.CheckpointIntegrator
import org.apache.hama.monitor.master.TaskArrivalEvent
import org.apache.hama.monitor.PublishEvent
import org.apache.hama.monitor.PublishMessage
import org.apache.hama.util.Utils._
import scala.collection.JavaConversions._
import scala.collection.immutable.Queue
import scala.util.Failure
import scala.util.Success
import scala.util.Try

sealed trait JobPlannerMessage
final case object Next extends JobPlannerMessage with Tick
final case class GetTargetRefs(infos: Array[SystemInfo]) 
      extends JobPlannerMessage
// TODO: merge TargetRefs and SomeMatched into e.g. TargetRefsFound
final case class TargetRefs(refs: Array[ActorRef]) extends JobPlannerMessage
final case class SomeMatched(matched: Array[ActorRef],
                             nomatched: Array[String]) 
      extends JobPlannerMessage
final case class FindGroomsToKillTasks(infos: Set[SystemInfo]) 
      extends JobPlannerMessage
final case class GroomsToKillFound(matched: Set[ActorRef], 
                                   nomatched: Set[String])
      extends JobPlannerMessage
final case class FindGroomsToRestartTasks(infos: Set[SystemInfo]) 
      extends JobPlannerMessage
final case class GroomsToRestartFound(matched: Set[ActorRef], 
                                   nomatched: Set[String])
      extends JobPlannerMessage
final case class TaskCancelled(taskAttemptId: String) extends JobPlannerMessage
final case class FindTasksAliveGrooms(infos: Set[SystemInfo]) 
      extends JobPlannerMessage
final case class TasksAliveGrooms(grooms: Set[ActorRef])
      extends JobPlannerMessage

final case class FindLatestCheckpoint(jobId: BSPJobID) 
      extends JobPlannerMessage
final case class LatestCheckpoint(jobId: BSPJobID, superstep: Long)
      extends JobPlannerMessage

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
  
protected[master] object JobManager {

  protected val stages = List(TaskAssign, Processing, Finished)

  def create(): JobManager = new JobManager()

}
// TODO: refactor for more compact api?
protected[master] class JobManager extends CommonLog {

  import JobManager._

  type NextStage = Stage
 
  type JobId = String 

  protected var taskAssignQueue = Queue[Ticket]()  
  
  protected var processingQueue = Queue[Ticket]() 

  protected var finishedQueue = Queue[Ticket]() 

  protected var command = Map.empty[JobId, Command] 

  protected var activeScheduleFinished = false 

  protected[master] def markScheduleFinished() = activeScheduleFinished =
    true

  protected[master] def rewindToBeforeSchedule() = activeScheduleFinished = 
    false

  protected[master] def allowPassiveAssign(): Boolean = true ==
    activeScheduleFinished 

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

  /**
   * Return the stored in the head of corresponded stage.
   */
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

  protected[master] def findTicketById(jobId: BSPJobID): 
    (Option[Stage], Option[Ticket]) = findTicketBy(TaskAssign, jobId) match {
      case Some(ticket) => (Option(TaskAssign), Option(ticket))
      case None => findTicketBy(Processing, jobId) match {
        case Some(ticket) => (Option(Processing), Option(ticket))
        case None => findTicketBy(Finished, jobId) match {
          case Some(ticket) => (Option(Finished), Option(ticket))
          case None => (None, None)
        }
      }
    }

  protected[master] def findTicketBy(stage: Stage, jobId: BSPJobID): 
    Option[Ticket] = stage match {
    case TaskAssign => if(!isEmpty(TaskAssign)) {
      val ticket = taskAssignQueue.head
      val job = ticket.job
      if(job.getId.equals(jobId)) Option(ticket) else None
    } else None
    case Processing => if(!isEmpty(Processing)) { 
      val ticket = processingQueue.head
      val job = ticket.job
      if(job.getId.equals(jobId)) Option(ticket) else None
    } else None
    case Finished => if(!isEmpty(Finished)) {
      val ticket = finishedQueue.head
      val job = ticket.job
      if(job.getId.equals(jobId)) Option(ticket) else None
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

  /**
   * No verification between next stage and target stage because a job may 
   * have one or more tasks fail that leads to the job is moved back to 
   * task assign stage.
   */
  protected[master] def move(jobId: BSPJobID)(target: Stage): Boolean = 
    findJobById(jobId) match {
      case (s: Some[Stage], j: Some[Job]) => dequeue(s.get) match {
        case Some(ticket) => { enqueue(target, ticket); true }
        case None => false
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

object JobPlanner {

  def simpleName(conf: HamaConfiguration): String = conf.get(
    "master.scheduler.name",
    classOf[JobPlanner].getSimpleName
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
class JobPlanner(setting: Setting, master: ActorRef, receptionist: ActorRef,
                federator: ActorRef) extends LocalService with Periodically {

  protected val jobManager = JobManager.create

  /* a guard for checking if all active tasks are scheduled. */
  protected var activeFinished = false 

  /** 
   * A cache when asking tracker the num of free slots available per active 
   * groom. 
   * Note that it's an array because multiple tasks may be dispatched to the
   * same groom server. 
   */
  protected var activeGrooms: Option[Array[ActorRef]] = None

  // TODO: move finished jobs to Federator's JobHistoryTracker(?)
  //       store job's metadata e.g. setting, state, etc. only

  override def initializeServices = {
    master ! SubscribeEvent(GroomLeaveEvent, RequestTaskEvent, TaskFailureEvent)
    federator ! SubscribeEvent(TaskArrivalEvent) 
    LOG.debug("Listening to groom leave, request task, and task failure, " +
              "task arrival events!")
    tick(self, Next)
  }

  /**
   * Periodically check if pulling a job for processing is needed.
   * @param message denotes which action to execute.
   */
  override def ticked(message: Tick) = message match { 
    case Next => if(jobManager.readyForNext)
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
    case null => noActiveGroomsConfigured(job)
    case _ => job.targetGrooms.length match { 
      case 0 => noActiveGroomsConfigured(job)
      case _ => schedule(job)
    }
  }

  protected def noActiveGroomsConfigured(job: Job) {
    activeFinished = true
    LOG.info("No active scheduling is required for job {}", job.getId)
  }

  // TODO: allow impl to obtain stats, etc. from tracker   
  protected def schedule(job: Job) {
    activeFinished = false 
    val targetGrooms = job.targetInfos  
    LOG.debug("{} requests target grooms refs {} for scheduling!", 
             name, targetGrooms.mkString(","))
    master ! GetTargetRefs(targetGrooms)
  }
 
  /**
   * In TaskAssign stage, target groom references found. Next step is to 
   * send the directive to target groom servers.
   * 
   * Note: active tasks may schedule to the same target groom servers when
   * only a task fails instead of groom.
   * 
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
          case null => 
          case task@_ => {
            val (host, port) = targetHostPort(ref)
            LOG.debug("Task {} is scheduled to target host {} port {}", 
                      task.getId, host, port)
            task.scheduleTo(host, port)
            val (r, t) = beforeActiveScheduleTask(ref, task)
            val (r1, t1) = activeScheduleTaskInternal(r, t)
            afterActiveScheduleTask(r1, t1)
            whenAllTasksScheduled(ticket)
          }
        })
      }}
      activeFinished = true
    } else LOG.error("No job exists at TaskAssign stage when active grooms "+
                     "found !")

  protected def beforeActiveScheduleTask(ref: ActorRef, task: Task): 
    (ActorRef, Task) = (ref, task)

  protected def activeScheduleTaskInternal(ref: ActorRef, task: Task): 
      (ActorRef, Task) = {
    task.getId.getId match {
      case id if 1 < id => ref ! new Directive(Resume, task, setting.name) 
      case _ => ref ! new Directive(Launch, task, setting.name)
    }
    (ref, task)
  }

  protected def afterActiveScheduleTask(ref: ActorRef, task: Task) { }

  protected def whenAllTasksScheduled(ticket: Ticket): Boolean =
    if(ticket.job.allTasksAssigned) {
      jobManager.moveToNextStage(ticket.job.getId) match {
        case (true, _) => if(jobManager.update(ticket.newWith(ticket.job.
                           newWithRunningState))) {
          LOG.info("All tasks in job {} are scheduled!", ticket.job.getId) 
          true
        } else false
        case _ => { LOG.error("Unable to move job {} to next stage!", 
                              ticket.job.getId); false }
      }
    } else false
  
  protected def cleanCachedActiveGrooms() = activeGrooms = None

  protected def cacheActiveGrooms(refs: Array[ActorRef]) = 
    activeGrooms = Option(refs) 
  
  protected def activeGroomsCached(): Array[ActorRef] = activeGrooms match {
    case Some(ref) => ref
    case None => throw new RuntimeException("Active grooms is missing!")
  }

  /**
   * During TaskAssign Stage, master replies scheduler's requesting groom 
   * references, by GetTargetRefs message, for active tasks.
   */
  // TODO: merge SomeMatched to TargetRefs 
  protected def activeTargetGrooms: Receive = { 
    case TargetRefs(refs) => {
      cacheActiveGrooms(refs) 
      federator ! AskFor(GroomsTracker.fullName, GetGroomCapacity(refs))
    }
    case SomeMatched(matched, nomatched) => if(!jobManager.isEmpty(TaskAssign))
      jobManager.headOf(TaskAssign).map { ticket => {
        ticket.client ! Reject("Grooms "+nomatched.mkString(", ")+
                               " do not exist!")
        jobManager.move(ticket.job.getId)(Finished) 
      }} else LOG.error("Can't schedule because TaskAssign queue is empty!")
    /** 
     * GroomCapacity is replied by GroomsTracker, after GetGroomCapacity. 
     * Note that activeGroomsCached may contain the same groom multiple times. 
     */
    case GroomCapacity(mapping: Map[ActorRef, Int]) =>  
      allGroomsHaveFreeSlots(mapping) match {
        case yes if yes.isEmpty => targetRefsFound(activeGroomsCached)
        case no if !no.isEmpty => preSlotUnavailable(no) 
      }
  }

  /**
   * When the GroomServer (actually TaskCounsellor) finds no free slots, it 
   * replies with NoFreeSlot message, denoteing the directive dispatched can't 
   * be executed.
   */
  protected def msgFromTaskCounsellor: Receive = {
    case msg: NoFreeSlot => msg.directive.task.isActive match {
      case true => postSlotUnavailable(sender, msg.directive)  
      /**
       * Passive assign always checks if requestng groom has enough free slot. 
       * So ideally it won't fall to this category.
       */
      case false => LOG.error("Passive assignment returns no free slots "+
                              "for directive {} from {}!", msg.directive, 
                              sender.path.address.hostPort) 
    }
  }

  protected def allGroomsHaveFreeSlots(mapping: Map[ActorRef, Int]): 
    Set[ActorRef] = mapping.filter { case (k, v) => v == 0 }.
                            map { case (k, v) => k }.toSet

  // TODO: to avoid active scheduling to grooms with insufficient slots, due to
  //       delay reporting, probably changing to allow schduler to update slots
  //       in tracker directly after assign or schedule function executed.
  protected def preSlotUnavailable(notEnough: Set[ActorRef]) = 
    jobManager.ticketAt match {
      case (s: Some[Stage], t: Some[Ticket]) => {
        val grooms = notEnough.map { ref => 
          val (host, port) = targetHostPort(ref) 
          host+":"+port
        }.toArray.mkString(",")
        val ticket = t.get
        val newTicket = ticket.newWith(ticket.job.newWithFailedState)
        jobManager.update(newTicket)
        jobManager.move(newTicket.job.getId)(Finished)
        newTicket.client ! Reject("Grooms "+grooms+" do not have free slots!")
        cleanCachedActiveGrooms 
        activeFinished = false
      }
      case (s@_, t@_) => throw new RuntimeException("Invalid ticket " + t +
                                                    " or stage " + s + "!") 
    }

  protected def postSlotUnavailable(groom: ActorRef, d: Directive) = 
    jobManager.ticketAt match {
      case (s: Some[Stage], t: Some[Ticket]) => 
        t.get.job.findTaskBy(d.task.getId) match {
          case null => throw new RuntimeException("No matched task "+
                                                  d.task.getId+" for reply "+
                                                  " from "+groom.path.name)
          case task@_ => {
            val (host, port) = targetHostPort(groom)
            val killing = t.get.job.newWithKillingState
            jobManager.update(t.get.newWith(killing)) 
            task.failedState
            jobManager.cacheCommand(t.get.job.getId.toString, 
                                    KillJob(host, port))
            val grooms = t.get.job.tasksRunAtExcept(task)
            val groomsAlive = toSet[SystemInfo](grooms)
            master ! FindGroomsToKillTasks(groomsAlive)
          }
        } 
      case (s@_, t@_) => throw new RuntimeException("Invalid ticket " + t +
                                                    " or stage " + s + "!") 
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
  // TODO: instead of checking activeFinished, each time when groom requests 
  //       check if sender is from target grooms (calcuate remaining free 
  //       slots in target grooms, but this may be more complicated).  
  def requestTask: Receive = {
    case req: RequestTask => {
      beforePassiveAssign(req, sender)
      if(activeFinished) passiveAssign(req.stats, sender) 
      afterPassiveAssign(req, sender)
    }
  } 

  protected def beforePassiveAssign(req: RequestTask, from: ActorRef) = 
    LOG.debug("GroomServer from {} at {}:{} requests for assigning a task "+
              "with activeFinished flag set to {}.", from.path.name, 
              req.stats.map { s => s.host }, req.stats.map { s=> s.port }, 
              activeFinished)

  protected def afterPassiveAssign(req: RequestTask, from: ActorRef) { }

  protected def passiveAssign(stats: Option[GroomStats], from: ActorRef) = 
    jobManager.isEmpty(TaskAssign) match {
      case false => jobManager.headOf(TaskAssign).map { ticket => 
        stats.map { s => assign(ticket, s, from) }}
      case true => LOG.debug("No task needs to schedule because job doesn't "+
                             "stay at task assign stage!")
    }  

  /**
   * Note that task assignment already consdiers multiple requests from the   
   * same groom. For instance, groom server A has 3 slots available. It then
   * issues 5 task requests. assign function will 
   * a. check the max tasks allowed (i.e. slot size - broken) by the groom A.
   * b. check the number of tasks already assigned to groom A.
   * c. assign task when a - b still has free slot available; otherwise drop
   *    request.
   */
  protected def assign(ticket: Ticket, stats: GroomStats, from: ActorRef) {
    val job = ticket.job
    val currentTasks = job.getTaskCountFor(stats.hostPort)
    val maxTasksAllowed = stats.maxTasks
    LOG.debug("Currently there are {} tasks at {}, with max {} tasks allowed.", 
             currentTasks, stats.host, maxTasksAllowed)
    (maxTasksAllowed >= (currentTasks+1)) match {
      case true => job.nextUnassignedTask match {
        case null =>         
        case task@_ => {
          val (host, port) = targetHostPort(from)
          LOG.debug("Task {} is assigned with target host {} port {}", 
                   task.getId, host, port)
          task.assignedTo(host, port)
          val (f, t) = beforePassiveAssignTask(from, task)
          val (f1, t1) = passiveAssignTaskInternal(f, t) 
          afterPassiveAssignTask(f1, t1)
          whenAllTasksAssigned(ticket) match {
            case true => afterAllTasksAssigned(ticket)
            case false => allTasksAssignedFail(ticket) 
          }
        }
      }
      case false => dropAssignRequest(stats, maxTasksAllowed, from)
    }
  }

  protected def beforePassiveAssignTask(from: ActorRef, task: Task): 
    (ActorRef, Task) = (from, task) 

  protected def passiveAssignTaskInternal(from: ActorRef, task: Task): 
      (ActorRef, Task) = {
    task.getId.getId match {
      case id if 1 < id => from ! new Directive(Resume, task, setting.name) 
      case _ => from ! new Directive(Launch, task, setting.name)
    }
    (from, task)
  }

  protected def afterPassiveAssignTask(from: ActorRef, task: Task) { }

  protected def dropAssignRequest(stats: GroomStats, 
                                  maxTasksAllowed: Int, 
                                  from: ActorRef) =
    LOG.warning("Drop GroomServer {} requests for a new task "+ 
                "because the number of tasks exceeds {} "+
                "allowed!", stats.host, maxTasksAllowed)

  protected def afterAllTasksAssigned(ticket: Ticket) { }

  protected def allTasksAssignedFail(ticket: Ticket) { }

  protected def whenAllTasksAssigned(ticket: Ticket): Boolean = 
    if(ticket.job.allTasksAssigned) {
      LOG.debug("Tasks for job {} are all assigned!", ticket.job.getId)
      jobManager.moveToNextStage(ticket.job.getId) match { 
        case (true, _) => if(jobManager.update(ticket.
                             newWith(ticket.job.newWithRunningState))) {
          LOG.info("All tasks assigned, job {} is running!", ticket.job.getId)
          true 
        } else false
        case _ => { LOG.error("Unable to move job {} to next stage!", 
                              ticket.job.getId); false }
      }
    } else false

  /**
   * GroomLeave event happens with some tasks run on the failed groom
   * Tghen following actions are taken:
   * - mark tasks running on the failed groom to failed state.
   * - note kill command for later reference.
   * - find grooms where rest tasks are running.
   * - rest operations are executed when GroomsToKillFound.
   */
  protected def whenActiveTasksFail(host: String, port: Int, ticket: Ticket,
                                    failedTasks: java.util.List[Task]) {
    val killing = ticket.job.newWithKillingState
    jobManager.update(ticket.newWith(killing)) 
    failedTasks.foreach( task => task.failedState)
    jobManager.cacheCommand(ticket.job.getId.toString, KillJob(host, port))
    val infos = ticket.job.tasksRunAt
    val groomsAlive = toSet[SystemInfo](infos).filterNot( info => 
      ( host + port ).equals( info.getHost + info.getPort )
    )  
    LOG.debug("Grooms still alive when active tasks {}:{} fail => {}", 
              host, port, groomsAlive)
    master ! FindGroomsToKillTasks(groomsAlive)
  } 

  protected def someTasksFail(host: String, port: Int, ticket: Ticket, 
                              stage: Stage, failedTasks: List[Task]) =
    failedTasks.exists(task => task.isActive) match { 
      case true => whenActiveTasksFail(host, port, ticket, failedTasks)
      case false => onlyPassiveTasksFail(host, port, ticket, failedTasks)
    }

  protected def events: Receive = {
    /**
     * Check if a job is in recovering state, and skip if true because all tasks
     * should receive kill directive and then are rescheduled accordingly.
     * 
     * Then check if any tasks fail on the groom by tasks size found.
     */
    case GroomLeave(name, host, port) => jobManager.ticketAt match {
      case (s: Some[Stage], t: Some[Ticket]) => t.get.job.isRecovering match {
        case false => toList(t.get.job.findTasksBy(host, port)) match {
          case list if list.isEmpty => LOG.debug("No tasks run on failed "+
                                                 "groom {}:{}!", host, port)
          case failedTasks if !failedTasks.isEmpty => 
            someTasksFail(host, port, t.get, s.get, failedTasks)
        }
        case true => t.get.job.getState match {
          case KILLING => toList(t.get.job.findTasksBy(host, port)) match {
            case list if list.isEmpty => LOG.debug("No tasks run on failed "+
                                                   "groom {}:{}!", host, port)
            case failedTasks if !failedTasks.isEmpty => { 
              failedTasks.foreach( failed => failed.failedState )
              if(t.get.job.allTasksStopped) allTasksKilled(t.get)
            }
          }
          case RESTARTING => toList(t.get.job.findTasksBy(host, port)) match {
            case list if list.isEmpty => LOG.debug("No tasks run on failed "+
                                                   "groom {}:{}!", host, port)
            case tasks if !tasks.isEmpty => tasks.exists({ failed => 
              failed.isActive 
            }) match {
              /**
               * Failed groom contains active tasks, the system is unable to
               * restart those tasks at the failed groom. So switch to killing
               * state.
               */
              case true => { 
                tasks.foreach( failed => failed.failedState )
                val killing = t.get.job.newWithKillingState
                jobManager.update(t.get.newWith(killing)) 
                if(t.get.job.allTasksStopped) allTasksKilled(t.get) 
              }
              case false => {
                tasks.foreach( failed => failed.failedState )
                if(t.get.job.allTasksStopped) beforeRestart(t.get)
              }  
            }
          }
        }
      }
      case _ => LOG.warning("No job existed!")
    } 
    /**
     * This happens when a groom leaves.
     * JobPlanner asks master for grooms references where tasks are running by 
     * issuing FindGroomsToKillTasks.
     * Grooms found already exclude failed groom server.
     * Once receiving grooms references, issue kill command to groom servers.
     */
    case GroomsToKillFound(matched, nomatched) => if(!nomatched.isEmpty) 
      LOG.error("Some grooms {} not found!", nomatched.mkString(",")) 
      else jobManager.ticketAt match {
        case (s: Some[Stage], t: Some[Ticket]) => matched.foreach( ref => {
          val (host, port) = targetHostPort(ref)
          t.get.job.findTasksBy(host, port).foreach ( task => 
            if(!task.isFailed) ref ! new Directive(Cancel, task, setting.name) 
          )
        })
        case (s@_, t@_) => throw new RuntimeException("Invalid stage "+s+
                                                      " or ticket "+t+"!")
      }
    /**
     * This happens when a groom leaves.
     */
    case GroomsToRestartFound(matched, nomatched) => if(!nomatched.isEmpty) 
      LOG.error("Can't stop for grooms {} not found!", nomatched.mkString(",")) 
      else jobManager.ticketAt match {
        case (s: Some[Stage], t: Some[Ticket]) => matched.foreach( ref => {
          val (host, port) = targetHostPort(ref)
          t.get.job.findTasksBy(host, port).foreach ( task => {
            if(!task.isFailed) ref ! new Directive(Cancel, task, setting.name) 
          })
        })
        case (s@_, t@_) => throw new RuntimeException("Invalid stage " + s +
                                                      " or ticket " + t + "!")
      } 
    /**
     * This happens when the groom leave event is triggered and where 
     * FindGroomsToKillTask is issued with active tasks found or task exceedng 
     * max attempt upper bound.
     * 
     * In processing queue:
     * - mark alive task as cancelled (not failed).
     * - check if all tasks in the job are stopped, either cancelled or failed.
     * If all tasks are marked as either cancelled or failed:
     *  - issue job finished event.
     *  - move job from processing queue to finished queue.
     *  - notify client.
     */
    case TaskCancelled(taskAttemptId) => jobManager.ticketAt match {
      case (stage: Some[Stage], t: Some[Ticket]) => 
        cancelTask(t.get, taskAttemptId)
      case _ => LOG.error("No ticket found at any Stage!")
    }
    /**
     * Update task in task table.
     * Check if all tasks are successful
     * Call broadcastFinished if all tasks are succeeded.
     * Move the job to Finished stage.
     */
    // TODO: job update function returns progress instead of boolean. so that
    //       progress can be used to notify client by e.g. client ! Progress(..)
    case newest: Task => jobManager.ticketAt match {
      case (s: Some[Stage], t: Some[Ticket]) => t.get.job.update(newest) match {
        case true => if(t.get.job.allTasksSucceeded) {
          val newJob = t.get.job.newWithSucceededState.newWithFinishNow
          jobManager.update(t.get.newWith(newJob))
          broadcastFinished(newJob.getId) 
          jobManager.move(newJob.getId)(Finished)
          notifyJobComplete(t.get.client, newJob.getId) 
        }
        case false => LOG.warning("Unable to update task {}!", newest.getId)
      }
      case _ => LOG.warning("No job existed!")
    }
    /**
     * Task failure should happen when the job is in Processing stage.
     * Find associated job (via task) in corresponded stage.
     * Mark the job as recovering.
     * Find groom references where tasks are still running.
     * Rest operation is dealt in TasksAliveGrooms (stop tasks, etc.)
     * Note that active tasks can be restarted at the original grooms assigned
     * because it's not groom failure.
     */
    case fault: TaskFailure => {
      jobManager.findJobById(fault.taskAttemptId.getJobID) match {
        case (s: Some[Stage], j: Some[Job]) => j.get.isRecovering match {
          /**
           * Directly mark the job as restarting because active tasks can be 
           * rescheduled to the original groom, which is still online. 
           */
          case false => firstTaskFail(s.get, j.get, fault.taskAttemptId)
          case true => j.get.getState match {
            case KILLING => j.get.findTaskBy(fault.taskAttemptId) match {
              case null => throw new RuntimeException("No matched task for "+
                                                      fault.taskAttemptId)
              case task@_ => {
                task.failedState
                if(j.get.allTasksStopped) jobManager.ticketAt match {
                  case (s: Some[Stage], t: Some[Ticket]) =>
                    allTasksKilled(t.get) 
                  case _ => throw new RuntimeException("No ticket found!")
                }
              }
            }
            case RESTARTING => j.get.findTaskBy(fault.taskAttemptId) match { 
              case null => throw new RuntimeException("No matched task for "+
                                                      fault.taskAttemptId)
              case task@_ => {
                task.failedState
                if(j.get.allTasksStopped) jobManager.ticketAt match {
                  case (s: Some[Stage], t: Some[Ticket]) => beforeRestart(t.get)
                  case _ => throw new RuntimeException("No ticket found!")
                }
              }
            }
          }
        }
        case _ => LOG.error("No matched job: {}", fault.taskAttemptId.getJobID)
      }
    }
    /**
     * Send Cancel directive with old task to groom when a task fail. 
     * Wait for TaskCancelled messages replied by grooms where tasks still 
     * alive.
     */
    case TasksAliveGrooms(grooms) => jobManager.ticketAt match {
      case (s: Some[Stage], t: Some[Ticket]) => 
        tasksAliveGroomsFound(grooms, t.get.job, beforeCancelTask, 
                              afterCancelTask)
      case (s@_, t@_) => LOG.error("Invalid stage {} or ticket {}!", s, t)
    }
  }

  /**
   * Mark the job as restarting.
   * Find corresponded task by task attempt id.
   * Mark the task as failure.
   * Find tasks alive on grooms where they are still running.
   * Rest operation is dealt in TasksAliveGrooms.
   */
  protected def firstTaskFail(stage: Stage, job: Job, faultId: TaskAttemptID) {
    markJobAsRestarting(stage, job)  
    val failed = job.findTaskBy(faultId) 
    if(null == failed)
      throw new NullPointerException("Not task found with failed id "+
                                     faultId)
    failed.failedState
    val aliveGrooms = toSet[SystemInfo](job.tasksRunAtExcept(failed))
    master ! FindTasksAliveGrooms(aliveGrooms)
  }

  protected def beforeCancelTask(groom: ActorRef, forRestart: Task): 
    (ActorRef, Task) = (groom, forRestart) 

  protected def afterCancelTask(groom: ActorRef, forRestart: Task) { }

  protected def tasksAliveGroomsFound(grooms: Set[ActorRef], job: Job,
      b: (ActorRef, Task) => (ActorRef, Task), a: (ActorRef, Task) => Unit) = 
    grooms.foreach( groom => {
      val (host, port) = targetHostPort(groom)
      job.findTasksBy(host, port).foreach ( taskToRestart => {
        val (g, t) = b(groom, taskToRestart)
        if(!taskToRestart.isFailed) g ! new Directive(Cancel, t, setting.name)
        a(g, t)
      })
    })


  /**
   * Mark the replied task attempt id as cancelled; then check if all tasks
   * are stopped - either cancelled or failed.
   * If all tasks are stopped, check the job state in deciding to kill the job
   * or perform restarting procedure.
   * @param ticket contains job and client.
   * @param taskAttemptId is the task to be marked as cancelled.
   */
  protected def cancelTask(ticket: Ticket, taskAttemptId: String) = 
    ticket.job.markCancelledWith(taskAttemptId) match { 
      case true => if(ticket.job.allTasksStopped) whenAllTasksStopped(ticket)
      case false => LOG.error("Unable to mark task {} killed!", taskAttemptId)
    }

  protected def whenAllTasksStopped(ticket: Ticket) = 
    ticket.job.getState match {
      case KILLING => allTasksKilled(ticket)
      case RESTARTING => beforeRestart(ticket)
    }

  protected def beforeRestart(ticket: Ticket) = 
    federator ! AskFor(CheckpointIntegrator.fullName, 
                       FindLatestCheckpoint(ticket.job.getId))

  /**
   * Following actions are taken:
   * - mark the job as failed and set finish time to current timemillis.
   * - broadcast job is finished, so tracker can update its state accordigly.
   * - move the job to Finished stage
   * - notify client that its' job has failed.
   */
  protected def allTasksKilled(ticket: Ticket) { 
    val job = ticket.job
    val newJob = job.newWithFailedState.newWithFinishNow 
    jobManager.update(ticket.newWith(newJob))
    val jobId = newJob.getId
    broadcastFinished(jobId) 
    jobManager.move(jobId)(Finished)
    jobManager.getCommand(jobId.toString) match {
      case Some(found) if found.isInstanceOf[KillJob] => 
        ticket.client ! Reject(found.asInstanceOf[KillJob].reason)  
      case Some(found) if !found.isInstanceOf[KillJob] => {
        LOG.warning("Unknown command {} to react for job {}", found, 
                    jobId)
        ticket.client ! Reject("Job "+jobId.toString+" fails!")  
      }
      case None => LOG.warning("No matched command for job {} in "+
                               "reacting to cancelled event!", jobId)
    }
  }

  protected def markJobAsRestarting(stage: Stage, job: Job) =
    jobManager.headOf(stage).map { ticket => 
      jobManager.update(ticket.newWith(job.newWithRestartingState))  
    }

  protected def markJobAsRunning(jobId: BSPJobID) = 
    jobManager.findJobById(jobId) match {
      case (s: Some[Stage], j: Some[Job]) => jobManager.headOf(s.get).map { t=>
        jobManager.update(t.newWith(j.get.newWithRunningState))  
      }
      case _ => LOG.error("Fail to mark job {} as running!", jobId)
    }

  /** 
   * Instead of rearrange and move job back to task assign stage immediately:
   * - mark tasks as failed first.
   * - find grooms where tasks are still up running. 
   * - wait for TasksToRestartGrooms returned.
   */
  protected def onlyPassiveTasksFail(host: String, port: Int, ticket: Ticket,
                                     failedTasks: java.util.List[Task]) {
    val restarting = ticket.job.newWithRestartingState
    jobManager.update(ticket.newWith(restarting)) 
    failedTasks.foreach( task => task.failedState)
    val infos = ticket.job.tasksRunAt
    LOG.debug("Grooms on which tasks are currently running: {}. "+
             "And failed groom: {}:{}", infos.mkString(", "), host, port)
    val groomsAlive = toSet[SystemInfo](infos).filterNot( info => 
      ( host + port ).equals( info.getHost + info.getPort )
    ) 
    LOG.info("Grooms still alive when passive tasks at {}:{} fail => {}", 
              host, port, groomsAlive)
    master ! FindGroomsToRestartTasks(groomsAlive)
  }

  /**
   * This function is called when a job if finished its execution.
   */
  protected def broadcastFinished(jobId: BSPJobID) =  
    federator ! JobFinishedMessage(jobId) 

  /**
   * beforeRestart function calls to FindLatestCheckpoint. Tracker replies
   * LatestCheckpoint message.
   */
  protected def msgFromFederator: Receive = {
    case LatestCheckpoint(jobId: BSPJobID, superstep: Long) => 
      whenRestart(jobId, superstep)
  }

  /**
   * After all tasks are stopped, either cancelled or failed, and beforeRestart
   * finds the latest checkpoint value, this function is then executed.
   * 
   * 
   * @param jobId is the job to be re-excecuted.
   * @param latest is the latest completed checkpoint value. 
   */
  protected def whenRestart(jobId: BSPJobID, latest: Long) = 
    Try(updateJob(jobId, latest)) match {
      case Success(job) => activeSchedule(job)
      case Failure(cause) => cause match {
        case e: TaskMaxAttemptedException => 
          jobManager.findTicketById(jobId) match {
            case (s: Some[Stage], t: Some[Ticket]) => { 
              jobManager.update(t.get.newWith(t.get.job.newWithFailedState))
              jobManager.move(jobId)(Finished)
              t.get.client ! Reject(e.toString)
            } 
            case _ => throw new RuntimeException("Can't find job "+jobId+
                                                 " when updating job data.")
          }
        case e: Exception => throw e 
      }
    }

  /**
   * Obtain ticket by job id. 
   * Update job's superstep to the latest checkpoint found in tracker.
   * Update all tasks' superstep, state, and marker values; and increment id.
   */  
  protected def updateJob(jobId: BSPJobID, latest: Long): Job = {
    jobManager.findTicketById(jobId) match {
      case (s: Some[Stage], t: Some[Ticket]) => {
        val jobWithLatestCheckpoint = t.get.job.newWithSuperstepCount(latest)
        jobWithLatestCheckpoint.allTasks.map { task =>
          jobWithLatestCheckpoint.newAttemptTask(task.withIdIncremented.
                                                      newWithSuperstep(latest).
                                                      newWithWaitingState.
                                                      newWithRevoke)
        }
        val newTicket = t.get.newWith(jobWithLatestCheckpoint)
        jobManager.update(newTicket) 
        jobManager.move(jobId)(TaskAssign) match {
          case true => newTicket.job
          case false => throw new RuntimeException("Unable to move job "+jobId)
        }
      }
      case _ => throw new RuntimeException("Can't find job "+jobId+" when "+
                                           "updating job data.")
    }
  }

  protected def notifyJobComplete(client: ActorRef, jobId: BSPJobID) =
    client ! JobComplete(jobId)

  override def receive = events orElse tickMessage orElse requestTask orElse dispense orElse activeTargetGrooms orElse msgFromTaskCounsellor orElse msgFromFederator orElse unknown

}
