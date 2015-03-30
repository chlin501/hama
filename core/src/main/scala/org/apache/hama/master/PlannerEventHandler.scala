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
import org.apache.hama.SystemInfo
import org.apache.hama.bsp.BSPJobID
import org.apache.hama.bsp.TaskAttemptID
import org.apache.hama.bsp.v2.Job
import org.apache.hama.bsp.v2.Job.State._
import org.apache.hama.bsp.v2.Task
import org.apache.hama.bsp.v2.TaskMaxAttemptedException
import org.apache.hama.client.JobComplete
import org.apache.hama.conf.Setting
import org.apache.hama.logging.CommonLog
import org.apache.hama.master.Directive.Action._
import org.apache.hama.monitor.master.CheckpointIntegrator
import org.apache.hama.util.Utils._
import scala.collection.JavaConversions._
import scala.util.Failure
import scala.util.Success
import scala.util.Try

object PlannerEventHandler {

  val default = classOf[DefaultPlannerEventHandler]

  def create(setting: Setting, jobManager: JobManager, master: ActorRef,
             federator: ActorRef, scheduler: Scheduler): PlannerEventHandler = {
    val cls = setting.hama.getClass("master.planner.handler", default,
                                    classOf[PlannerEventHandler])
    Try(cls.getConstructor(classOf[Setting], classOf[JobManager], 
      classOf[ActorRef], classOf[ActorRef], classOf[Scheduler]). 
      newInstance(setting, jobManager, master, federator, scheduler)) match {
      case Success(instance) => instance
      case Failure(cause) => throw cause
    }
  }

}

trait PlannerEventHandler {

  /**
   * Groom leave event.
   */
  def whenGroomLeaves(host: String, port: Int) 

  /**
   * Groom leave event.
   * React to GroomsToKillFound and GroomsToRestartFound.
   */
  def cancelTasks(matched: Set[ActorRef], nomatched: Set[String])

  /**
   * This happens when a groom replies the running task is cancelled.
   */
  def cancelled(ticket: Ticket, taskAttemptId: String)

  /**
   * Update task information.
   */
  def renew(newest: Task) 

  /**
   * Task failure event.
   */
  def whenTaskFails(taskAttemptId: TaskAttemptID)

  /**
   * Reply to FindTasksAliveGrooms message.
   */
  def tasksAliveAt(grooms: Set[ActorRef])

  /**
   * Ask federator for the latest checkpoint.
   */
  def whenRestart(jobId: BSPJobID, latest: Long)

}

protected[master] class DefaultPlannerEventHandler(setting: Setting, 
  jobManager: JobManager, master: ActorRef, federator: ActorRef, 
  scheduler: Scheduler) extends PlannerEventHandler with CommonLog {

  type Host = String
  type Port = Int

  protected def broadcastFinished(jobId: BSPJobID) =  
    federator ! JobFinishedMessage(jobId) 

  protected def allTasksKilled(ticket: Ticket) {
    val job = ticket.job
    val newJob = job.newWithFailedState.newWithFinishNow
    jobManager.update(ticket.newWith(newJob))
    val jobId = newJob.getId
    broadcastFinished(jobId) 
    jobManager.move(jobId)(Finished)
    jobManager.getCommand(jobId.toString) match { // TODO: refactor
      case Some(found) if found.isInstanceOf[KillJob] => {
        LOG.debug("Kill job {} and send back to client!", jobId)
        ticket.client ! Reject(found.asInstanceOf[KillJob].reason)
      }
      case Some(found) if !found.isInstanceOf[KillJob] => {
        LOG.warning("Unknown command {} to react for job {}", found,
                    jobId)
        ticket.client ! Reject("Job "+jobId.toString+" fails!")
      }
      case None => LOG.warning("No matched command for job {} in "+
                               "reacting to cancelled event!", jobId)
    }
  }

  protected def beforeRestart(ticket: Ticket) =
    federator ! AskFor(CheckpointIntegrator.fullName, 
                       FindLatestCheckpoint(ticket.job.getId))

  protected def whenAllTasksStopped(ticket: Ticket) = 
    ticket.job.getState match {
      case KILLING => allTasksKilled(ticket)
      case RESTARTING => beforeRestart(ticket)
    }

  override def cancelled(ticket: Ticket, taskAttemptId: String) = 
    ticket.job.markCancelledWith(taskAttemptId) match { 
      case true => if(ticket.job.allTasksStopped) whenAllTasksStopped(ticket)
      case false => LOG.error("Unable to mark task {} killed!", taskAttemptId)
    }

  protected def resolve(ref: ActorRef): (Host, Port) = {
    val host = ref.path.address.host.getOrElse("localhost")
    val port = ref.path.address.port.getOrElse(50000)
    (host, port)
  }

  override def cancelTasks(matched: Set[ActorRef], nomatched: Set[String]) =
    nomatched.isEmpty match {
      // TODO: kill tasks where grooms alive
      case false => LOG.error("Grooms {} not found!", nomatched.mkString(","))  
      case true => jobManager.ticketAt match {
        case (s: Some[Stage], t: Some[Ticket]) => matched.foreach( ref => {
          val (host, port) = resolve(ref)
          t.get.job.findTasksBy(host, port).foreach ( task => {
            if(!task.isFailed) ref ! new Directive(Cancel, task, "master") 
          })
        })
        case (s@_, t@_) => throw new RuntimeException("Invalid stage "+s+
                                                      " or ticket "+t+"!")
      }
    }

  override def whenGroomLeaves(host: String, // TODO: further refinement
                              port: Int) = jobManager.ticketAt match {
    case (s: Some[Stage], t: Some[Ticket]) => t.get.job.isRecovering match {
      case false => toList(t.get.job.findTasksBy(host, port)) match {
        case list if list.isEmpty => LOG.info("No failed tasks run at {}:{}!",
          host, port)
        /**
         * First time when some tasks fail on a specific groom.
         */
        case failedTasks if !failedTasks.isEmpty => someTasksFail(host, port, 
          t.get, s.get, failedTasks)
      }
      /**
       * Previously there was at least one groom fail, denoting Cancel command
       * logically should have been sent to groom servers.
       * Note if Cancel commands are not sent, that needs to be dealt in 
       * seperated events when necessary.
       */
      case true => t.get.job.getState match {
        case KILLING => toList(t.get.job.findTasksBy(host, port)) match {
          case list if list.isEmpty => LOG.info("Tasks doesn't fail at {}:{}!", 
            host, port) 
          case failedTasks if !failedTasks.isEmpty => {
            failedTasks.foreach( failed => failed.failedState )
            if(t.get.job.allTasksStopped) allTasksKilled(t.get)
          }
        }
        case RESTARTING => toList(t.get.job.findTasksBy(host, port)) match {
          case list if list.isEmpty => LOG.info("No tasks fail at {}:{}!",
            host, port)
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
            /**
             * Logically Cancel command should have been sent out to grooms, so
             * merely marking tasks on failed grooms as failed.
             */
            case false => {
              tasks.foreach( failed => failed.failedState )
              if(t.get.job.allTasksStopped) beforeRestart(t.get)
            }
          }
        }
      }
    }
    case _ => LOG.error("Unable to find corresponded ticket when {}:{} leaves",
                        host, port)
  } 

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

  protected def onlyPassiveTasksFail(host: String, port: Int, ticket: Ticket,
                                     failedTasks: java.util.List[Task]){
    LOG.debug("Only passive tasks fail. Groom fails at {}:{}", host, port)
    val restarting = ticket.job.newWithRestartingState
    jobManager.update(ticket.newWith(restarting))
    failedTasks.foreach( task => task.failedState)
    val infos = ticket.job.tasksRunAt
    LOG.debug("Tasks are currently running on {}. "+
             "And failed groom: {}:{}", infos.mkString(", "), host, port)
    val groomsAlive = toSet[SystemInfo](infos).filterNot( info =>
      ( host + port ).equals( info.getHost + info.getPort )
    )
    LOG.debug("Grooms still alive when passive tasks at {}:{} fail => {}",
              host, port, groomsAlive)
    master ! FindGroomsToRestartTasks(groomsAlive) 
  }

  protected def someTasksFail(host: String, port: Int, ticket: Ticket,
                              stage: Stage, failedTasks: List[Task]) =
    failedTasks.exists(task => task.isActive) match {
      case true => whenActiveTasksFail(host, port, ticket, failedTasks)
      case false => onlyPassiveTasksFail(host, port, ticket, failedTasks)
    }

  override def renew(newest: Task) = jobManager.ticketAt match {
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

  override def whenTaskFails(taskAttemptId: TaskAttemptID) = // TODO: refinement
    jobManager.findJobById(taskAttemptId.getJobID) match {
      case (s: Some[Stage], j: Some[Job]) => j.get.isRecovering match {
        /**
         * Directly mark the job as restarting because active tasks can be 
         * rescheduled to the original groom, which is still online. 
         */
        case false => firstTaskFails(s.get, j.get, taskAttemptId)
        case true => j.get.getState match {
          case KILLING => j.get.findTaskBy(taskAttemptId) match {
            case null => throw new RuntimeException("Dangling task "+
                                                    taskAttemptId+"!")
            case task@_ => {
              task.failedState
              if(j.get.allTasksStopped) jobManager.ticketAt match {
                case (s: Some[Stage], t: Some[Ticket]) =>
                  allTasksKilled(t.get) 
                case _ => throw new RuntimeException("Invalid ticket or stage!")
              }
            }
          }
          /**
           * Active tasks can be restarted at GroomSerers without a problem, so
           * checking if task is active or not is not necessary.
           */
          case RESTARTING => j.get.findTaskBy(taskAttemptId) match { 
            case null => throw new RuntimeException("Dangling task "+
                                                    taskAttemptId+" found!")
            case task@_ => {
              task.failedState
              if(j.get.allTasksStopped) jobManager.ticketAt match {
                case (s: Some[Stage], t: Some[Ticket]) => beforeRestart(t.get)
                case _ => throw new RuntimeException("Invalid ticket or stage!")
              }
            }
          }
        }
      }
      case _ => LOG.error("No matched job: {}", taskAttemptId.getJobID)
    }

  protected def firstTaskFails(stage: Stage, job: Job, faultId: TaskAttemptID) {
    markJobAsRestarting(stage, job)
    val failed = job.findTaskBy(faultId)
    if(null == failed)
      throw new NullPointerException("Not task found with failed id "+ faultId)
    failed.failedState
    val aliveGrooms = toSet[SystemInfo](job.tasksRunAtExcept(failed))
    LOG.debug("Grooms with tasks running are still alive: {}", 
             aliveGrooms.mkString(","))
    master ! FindTasksAliveGrooms(aliveGrooms) 
  }

  protected def markJobAsRestarting(stage: Stage, job: Job) =
    jobManager.headOf(stage).map { ticket =>
      jobManager.update(ticket.newWith(job.newWithRestartingState))
    }

  override def tasksAliveAt(grooms: Set[ActorRef]) = jobManager.ticketAt match {
    case (s: Some[Stage], t: Some[Ticket]) => tasksAliveGroomsFound(grooms, 
      t.get.job)
    case (s@_, t@_) => LOG.error("Invalid stage {} or ticket {}!", s, t)
  }

  protected def tasksAliveGroomsFound(grooms: Set[ActorRef], job: Job) = 
    grooms.foreach( groom => {
      val (host, port) = resolve(groom)
      job.findTasksBy(host, port).foreach ( task => if(!task.isFailed) 
        groom ! new Directive(Cancel, task, setting.name)
      )
    })

  protected def notifyJobComplete(client: ActorRef, jobId: BSPJobID) =
    client ! JobComplete(jobId)

  override def whenRestart(jobId: BSPJobID, latest: Long) =
    Try(updateJob(jobId, latest)) match {
      case Success(ticket) => if(scheduler.examine(ticket)) 
        scheduler.findGroomsFor(ticket, master)
      case Failure(cause) => cause match {
        case e: TaskMaxAttemptedException =>
          jobManager.findTicketById(jobId) match {
            case (s: Some[Stage], t: Some[Ticket]) => {
              LOG.error("Fail updating job because {}!", cause)
              jobManager.update(t.get.newWith(t.get.job.newWithFailedState))
              jobManager.move(jobId)(Finished)
              t.get.client ! Reject(e.toString)
            }
            case _ => throw cause
          }
        case e: Exception => throw e // TODO: client ! Reject(e.toString)
      }
    }

  protected def updateJob(jobId: BSPJobID, latest: Long): Ticket = {
    jobManager.findTicketById(jobId) match {
      case (s: Some[Stage], t: Some[Ticket]) => {
        val jobWithLatestCheckpoint = t.get.job.newWithSuperstepCount(latest)
        jobWithLatestCheckpoint.allTasks.map { task =>
          jobWithLatestCheckpoint.newAttemptTask(task.withIdIncremented.
                                                      newWithSuperstep(latest).
                                                      newWithWaitingState.
                                                      newWithRevoke)
        }
        val newTicket = t.get.newWith(jobWithLatestCheckpoint.
          newWithRunningState)
        jobManager.update(newTicket)
        jobManager.move(jobId)(TaskAssign) match {
          case true => newTicket
          case false => throw new RuntimeException("Unable to move job "+jobId)
        }
      }
      case _ => throw new RuntimeException("Can't find job "+jobId+" when "+
                                           "updating job data.")
    }
  }
 
}

