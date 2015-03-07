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
import org.apache.hadoop.util.ReflectionUtils
import org.apache.hama.bsp.v2.Task
import org.apache.hama.bsp.v2.Job
import org.apache.hama.HamaConfiguration
import org.apache.hama.SystemInfo
import org.apache.hama.master.Directive.Action._
import org.apache.hama.logging.CommonLog

object Scheduler {

  val default = classOf[DefaultScheduler]

  // TODO: change conf to setting; unify instance creation
  def create(conf: HamaConfiguration, jobManager: JobManager): Scheduler = 
    conf.getClass("bsp.scheduler.class", default, classOf[Scheduler]) match {
      case `default` => new DefaultScheduler(jobManager)
      case clazz@_ => ReflectionUtils.newInstance(clazz, conf)
    }

}

trait Scheduler {

  def receive(ticket: Ticket)

  def examine(ticket: Ticket): Boolean 

  def findGroomsFor(ticket: Ticket, master: ActorRef)

  def validate(job: Job, actual: Array[ActorRef]): Boolean

  def found(targetGrooms: Array[ActorRef])

  def beforeSchedule(to: ActorRef, task: Task): (ActorRef, Task)

  def internalSchedule(to: ActorRef, task: Task): (ActorRef, Task)

  def afterSchedule(to: ActorRef, task: Task)

  def finalize(ticket: Ticket): Boolean

}

protected[master] class DefaultScheduler(jobManager: JobManager) 
      extends Scheduler with CommonLog {

  override def receive(ticket: Ticket) = {
    jobManager.rewindToBeforeSchedule
    jobManager.enqueue(ticket) 
  }

  override def examine(ticket: Ticket): Boolean = 
    ticket.job.targetGrooms match {
      case null | _ if ticket.job.targetGrooms.isEmpty => {
        jobManager.markScheduleFinished
        false
      }
      case _ => true
    } 

  override def findGroomsFor(ticket: Ticket, master: ActorRef) { 
    val infos = ticket.job.targetInfos 
    LOG.debug("{} requests target grooms {} for scheduling!", 
              getClass.getSimpleName, infos.mkString(","))
    master ! GetTargetRefs(infos)
  }

  override def validate(job: Job, actual: Array[ActorRef]): Boolean = 
    job.targetInfos.size == actual.size

  override def beforeSchedule(to: ActorRef, task: Task): (ActorRef, Task) = {
    val host = to.path.address.host.getOrElse("")
    val port = to.path.address.port.getOrElse(50000)
    task.scheduleTo(host, port)
    (to, task)
  }

  protected def getTicket(): Option[Ticket] = 
    if(!jobManager.isEmpty(TaskAssign)) jobManager.headOf(TaskAssign) else None

  override def found(targets: Array[ActorRef]) = { getTicket match {
    case Some(t) => validate(t.job, targets) match {
      case true => targets.foreach { groom => t.job.nextUnassignedTask match {
        case null =>
        case task@_ => {
          val (g1, t1) = beforeSchedule(groom, task)
          val (g2, t2) = internalSchedule(g1, t1) 
          afterSchedule(g2, t2) 
          finalize(t)
        }
      }}
      case false => failValidation(t)
    }
    case None => LOG.error("No ticket found at TaskAssign stage!")
  }; jobManager.markScheduleFinished }

  // TODO: mark job failed, notify clinet, issue kill, then move job to finish queue?
  protected def failValidation(ticket: Ticket) = 
    LOG.error("Failing job validation!")
 
  override def internalSchedule(ref: ActorRef, t: Task): (ActorRef, Task) = {
    t.getId.getId match {
      case id if 1 < id => ref ! new Directive(Resume, t, "master")
      case _ => ref ! new Directive(Launch, t, "master")
    }
    (ref, t)
  }

  override def afterSchedule(a: ActorRef, t: Task) { }

  override def finalize(ticket: Ticket): Boolean = 
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

  
}
