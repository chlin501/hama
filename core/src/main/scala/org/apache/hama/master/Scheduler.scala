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
import scala.util.Failure
import scala.util.Success
import scala.util.Try

object Scheduler {

  val default = classOf[DefaultScheduler]

  // TODO: change conf to setting; unify instance creation
  def create(conf: HamaConfiguration, jobManager: JobManager): Scheduler = {
    val cls = conf.getClass("master.scheduler.class", default, 
                            classOf[Scheduler])
    Try(cls.getConstructor(classOf[JobManager]).newInstance(jobManager)) match {
      case Success(instance) => instance
      case Failure(cause) => throw cause
    }
  }

}

trait Scheduler {

  def receive(ticket: Ticket)

  def examine(ticket: Ticket): Boolean 

  def findGroomsFor(ticket: Ticket, master: ActorRef)

  def found(targets: Array[ActorRef]) 

}

protected[master] class DefaultScheduler(jobManager: JobManager) 
      extends Scheduler with CommonLog {

  override def receive(ticket: Ticket) = jobManager.enqueue(ticket) 

  override def examine(ticket: Ticket): Boolean = 
    ticket.job.targetGrooms match {
      case null | _ if ticket.job.targetGrooms.isEmpty => {
        jobManager.markScheduleFinished; false
      }
      case _ => { jobManager.rewindToBeforeSchedule; true }
    } 

  override def findGroomsFor(ticket: Ticket, master: ActorRef) { 
    val infos = ticket.job.targetInfos 
    LOG.debug("{} requests target grooms {} for scheduling!", 
              getClass.getSimpleName, infos.mkString(","))
    master ! GetTargetRefs(infos)
  }

  protected[master] def validate(job: Job, actual: Array[ActorRef]): Boolean = 
    job.targetInfos.size == actual.size

  protected[master] def before(to: ActorRef, task: Task): (ActorRef, Task) = {
    val (host, port) = resolve(to)
    task.scheduleTo(host, port)
    (to, task)
  }

  protected[master] def resolve(ref: ActorRef): (String, Int) = {
    val host = ref.path.address.host.getOrElse("localhost")
    val port = ref.path.address.port.getOrElse(50000)
    (host, port)
  }

  protected[master] def ticket(): Option[Ticket] = 
    if(!jobManager.isEmpty(TaskAssign)) jobManager.headOf(TaskAssign) else None

  override def found(targets: Array[ActorRef]) = try { ticket match {
    case Some(t) => validate(t.job, targets) match {
      case true => targets.foreach { groom => t.job.nextUnassignedTask match {
        case null =>
        case task@_ => {
          val (g1, t1) = before(groom, task)
          val (g2, t2) = internal(g1, t1) 
          after(g2, t2) 
          finalize(t)
        }
      }}
      case false => failValidation(t)
    }
    case None => ticketNotFound
  }} finally { jobManager.markScheduleFinished }

  // TODO: mark job fails. issue kill, then move job to finish queue?
  protected[master] def failValidation(ticket: Ticket) = 
    LOG.error("Failing validate job {}!", ticket.job.getId)
  
  protected[master] def ticketNotFound() =
    LOG.error("No ticket found at TaskAssign stage!")
 
  protected[master] def internal(ref: ActorRef, t: Task): (ActorRef, Task) = {
    t.getId.getId match {
      case id if 1 < id => ref ! new Directive(Resume, t, "master")
      case _ => ref ! new Directive(Launch, t, "master")
    }
    (ref, t) 
  }

  protected[master] def after(to: ActorRef, task: Task) { }

  protected[master] def finalize(ticket: Ticket): Boolean = 
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
