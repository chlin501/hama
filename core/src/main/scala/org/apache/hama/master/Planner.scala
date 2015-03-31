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

object Planner {

  def simpleName(conf: HamaConfiguration): String = conf.get(
    "master.planner.name", 
    classOf[Planner].getSimpleName
  )

}

class Planner(setting: Setting, master: ActorRef, receptionist: ActorRef,
              federator: ActorRef, scheduler: Scheduler, assigner: Assigner,
              jobManager: JobManager, event: PlannerEventHandler) 
      extends LocalService with Periodically {

  /** 
   * A cache when asking tracker the num of free slots available per active 
   * groom. 
   * Note that it's an array because multiple tasks may be dispatched to the
   * same groom server. 
   */
  // TODO: move to cached related object.
  protected var activeGrooms: Option[Array[ActorRef]] = None

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
      scheduler.receive(ticket) 
      scheduler.examine(ticket) match {
        case true => scheduler.findGroomsFor(ticket, master)
        case false => LOG.debug("Maybe no active tasks need to be scheduled!")
      }
    }
  }
 
  // TODO: move to cache related object.
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
  protected def activeTargets: Receive = { 
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
        case yes if yes.isEmpty => scheduler.found(activeGroomsCached)
        case no if !no.isEmpty => //preSlotUnavailable(no) 
      }
  }

  /**
   * When the GroomServer (actually TaskCounsellor) finds no free slots, it 
   * replies with NoFreeSlot message, denoteing the directive dispatched can't 
   * be executed.
   */
  protected def msgs: Receive = {
    case msg: NoFreeSlot => msg.directive.task.isActive match {
      case true => event.noFreeSlot(sender, msg.directive)  
      /**
       * Passive assign always checks if requestng groom has enough free slot. 
       * So ideally it won't fall to this category.
       */
      case false => LOG.error("Passive assignment returns no free slots "+
                              "for directive {} from {}!", msg.directive, 
                              sender.path.address.hostPort) 
    }
    /**
     * beforeRestart function calls to FindLatestCheckpoint. Tracker replies
     * LatestCheckpoint message.
     */
    case LatestCheckpoint(jobId: BSPJobID, superstep: Long) => 
      event.whenRestart(jobId, superstep)
  }

  protected def allGroomsHaveFreeSlots(mapping: Map[ActorRef, Int]): 
    Set[ActorRef] = mapping.filter { case (k, v) => v == 0 }.
                            map { case (k, v) => k }.toSet

/*
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

  protected def targetHostPort(ref: ActorRef): (String, Int) = {
    val host = ref.path.address.host.getOrElse("")
    val port = ref.path.address.port.getOrElse(50000)
    (host, port)
  }
*/

  /**
   * GroomServer's TaskCounsellor requests for assigning a task.
   * @return Receive partiail function.
   */
  // TODO: instead of checking activeFinished, each time when groom requests 
  //       check if sender is from target grooms (calcuate remaining free 
  //       slots in target grooms, but this may be more complicated).  
  def requestTask: Receive = {
    case req: RequestTask => assigner.examine(jobManager) match {
      case Some(ticket) => req.stats.map { stats => 
        if(assigner.validate(ticket, stats)) 
          assigner.assign(ticket, stats, sender)
      }
      case None => LOG.warning("Can't find ticket!")
    } 
  } 

  protected def events: Receive = {
    /**
     * Check if a job is in recovering state, and skip if true because all tasks
     * should receive kill directive and then are rescheduled accordingly.
     * 
     * Then check if any tasks fail on the groom by tasks size found.
     */
    case GroomLeave(name, host, port) => event.whenGroomLeaves(host, port) 
    /**
     * This happens when a groom leaves.
     * Planner asks master for grooms references where tasks are running by 
     * issuing FindGroomsToKillTasks.
     * Grooms found already exclude failed groom server.
     * Once receiving grooms references, issue kill command to groom servers.
     */
    case GroomsToKillFound(matched, nomatched) => event.cancelTasks(matched, 
      nomatched)
    /**
     * This happens when a groom leaves.
     */
    case GroomsToRestartFound(matched, nomatched) => event.cancelTasks(matched,
      nomatched)
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
      case (stage: Some[Stage], t: Some[Ticket]) => event.cancelled(t.get, 
        taskAttemptId)
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
    case newest: Task => event.renew(newest) 
    /**
     * Task failure should happen when the job is in Processing stage.
     * Find associated job (via task) in corresponded stage.
     * Mark the job as recovering.
     * Find groom references where tasks are still running.
     * Rest operation is dealt in TasksAliveGrooms (stop tasks, etc.)
     * Note that active tasks can be restarted at the original grooms assigned
     * because it's not groom failure.
     */
    case fault: TaskFailure => event.whenTaskFails(fault.taskAttemptId)
    /**
     * Send Cancel directive with old task to groom when a task fail. 
     * Wait for TaskCancelled messages replied by grooms where tasks still 
     * alive.
     */
    // TODO: call event.cancelTasks instead
    case TasksAliveGrooms(grooms) => event.cancelTasks(grooms, Set[String]())
  }

/*

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
*/

  /**
   * This function is called when a job if finished its execution.
   */
  protected def broadcastFinished(jobId: BSPJobID) =  
    federator ! JobFinishedMessage(jobId) 

  protected def notifyJobComplete(client: ActorRef, jobId: BSPJobID) =
    client ! JobComplete(jobId)

  override def receive = events orElse tickMessage orElse requestTask orElse dispense orElse activeTargets orElse msgs orElse unknown

}
