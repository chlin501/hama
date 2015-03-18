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
import org.apache.hama.bsp.v2.Job.State._
import org.apache.hama.bsp.v2.Task
import org.apache.hama.master.Directive.Action._
import org.apache.hama.logging.CommonLog
import org.apache.hama.util.Utils._
import scala.collection.JavaConversions._
trait EventHandler 

protected[master] object GroomEventHandler {

  protected[master] def create(jobManager: JobManager): GroomEventHandler = 
    new DefaultGroomEventHandler(jobManager)

} 

protected[master] trait GroomEventHandler extends EventHandler {

  def whenGroomLeave(host: String, port: Int) 

  /**
   * React to GroomsToKillFound and GroomsToRestartFound.
   */
  def cancelTasks(matched: Set[ActorRef], nomatched: Set[String])

  def confirmCancelled(taskAttemptId: String)

}

protected[master] class DefaultGroomEventHandler(jobManager: JobManager) 
  extends GroomEventHandler with CommonLog {

  protected[master] def resolve(ref: ActorRef): (String, Int) = {
    val host = ref.path.address.host.getOrElse("localhost")
    val port = ref.path.address.port.getOrElse(50000)
    (host, port)
  }

  protected[master] def whenAllTasksStopped(ticket: Ticket) = 
    ticket.job.getState match {
      case KILLING => allTasksKilled(ticket)
      case RESTARTING => beforeRestart(ticket)
    }

  protected[master] def confirm(ticket: Ticket, taskAttemptId: String) = 
    ticket.job.markCancelledWith(taskAttemptId) match { 
      case true => if(ticket.job.allTasksStopped) whenAllTasksStopped(ticket)
      case false => LOG.error("Unable to mark task {} killed!", taskAttemptId)
    }

  override def confirmCancelled(taskAttemptId: String) = 
    jobManager.ticketAt match {
      case (stage: Some[Stage], t: Some[Ticket]) => 
        confirm(t.get, taskAttemptId)
      case _ => LOG.error("No ticket found at any Stage!")
    }

  override def cancelTasks(matched: Set[ActorRef], nomatched: Set[String]) =
    nomatched.isEmpty match {
      // TODO: kill tasks where grooms alive
      case true => LOG.error("Some grooms {} not found!", 
                             nomatched.mkString(","))  
      case false => jobManager.ticketAt match {
        case (s: Some[Stage], t: Some[Ticket]) => matched.foreach( ref => {
          val (host, port) = resolve(ref)
          t.get.job.findTasksBy(host, port).foreach ( task => 
            if(!task.isFailed) ref ! new Directive(Cancel, task, "master") 
          )
        })
        case (s@_, t@_) => throw new RuntimeException("Invalid stage "+s+
                                                      " or ticket "+t+"!")
      }
    }

  override def whenGroomLeave(host: String, 
                              port: Int) = jobManager.ticketAt match {
    case (s: Some[Stage], t: Some[Ticket]) => t.get.job.isRecovering match {
      case false => toList(t.get.job.findTasksBy(host, port)) match {
        case list if list.isEmpty => 
        case failedTasks if !failedTasks.isEmpty => someTasksFail(host, port, 
          t.get, s.get, failedTasks)
      }
      case true => t.get.job.getState match {
        case KILLING => toList(t.get.job.findTasksBy(host, port)) match {
          case list if list.isEmpty => 
          case failedTasks if !failedTasks.isEmpty => {
            failedTasks.foreach( failed => failed.failedState )
            if(t.get.job.allTasksStopped) allTasksKilled(t.get)
          }
        }
        case RESTARTING => toList(t.get.job.findTasksBy(host, port)) match {
          case list if list.isEmpty => 
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
    case _ => LOG.error("Unable to find corresponded ticket when {}:{} leaves",
                        host, port)
  } 

  protected[master] def whenActiveTasksFail(host: String, port: Int, 
                                            ticket: Ticket, 
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
    //master ! FindGroomsToKillTasks(groomsAlive) TODO: 
  }

  protected[master] def onlyPassiveTasksFail(host: String, port: Int, 
                                             ticket: Ticket,
                                             failedTasks: java.util.List[Task]){
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
    //master ! FindGroomsToRestartTasks(groomsAlive)
  }

  protected[master] def someTasksFail(host: String, port: Int, ticket: Ticket,
                                      stage: Stage, failedTasks: List[Task]) =
    failedTasks.exists(task => task.isActive) match {
      case true => whenActiveTasksFail(host, port, ticket, failedTasks)
      case false => onlyPassiveTasksFail(host, port, ticket, failedTasks)
    }

  protected[master] def allTasksKilled(ticket: Ticket) {
    val job = ticket.job
    val newJob = job.newWithFailedState.newWithFinishNow
    jobManager.update(ticket.newWith(newJob))
    val jobId = newJob.getId
    //broadcastFinished(jobId)
    jobManager.move(jobId)(Finished)
    jobManager.getCommand(jobId.toString) match { // TODO: refactor
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

  protected[master] def beforeRestart(ticket: Ticket) { }
    //federator ! AskFor(CheckpointIntegrator.fullName,
                       //FindLatestCheckpoint(ticket.job.getId))

}

protected[master] object TaskEventHandler {

  protected[master] def create(jobManager: JobManager): TaskEventHandler = 
    new DefaultTaskEventHandler(jobManager)

}

protected[master] trait TaskEventHandler extends EventHandler {

}

protected[master] class DefaultTaskEventHandler(jobManager: JobManager) 
  extends TaskEventHandler with CommonLog {

}

