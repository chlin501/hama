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
import org.apache.hama.bsp.v2.Job
import org.apache.hama.bsp.v2.Task
import org.apache.hama.HamaConfiguration
import org.apache.hama.logging.CommonLog
import org.apache.hama.master.Directive.Action._
import org.apache.hama.monitor.GroomStats
import scala.util.Failure
import scala.util.Success
import scala.util.Try

object Assigner {

  val default = classOf[DefaultAssigner]

  /**
   * All instance should accept JobManager as parameter. 
   */
  // TODO: change conf to setting. unify instance creation
  def create(conf: HamaConfiguration, jobManager: JobManager): Assigner = {
    val cls = conf.getClass("assigner.class", default, classOf[Assigner])
    Try(cls.getConstructor(classOf[JobManager]).newInstance(jobManager)) match {
      case Success(instance) => instance
      case Failure(cause) => throw cause 
    }
  }

}

trait Assigner {

  def examine(jobManager: JobManager): Option[Ticket] 

  def dropRequest(stats: GroomStats, target: ActorRef)

  def validate(job: Job, stats: GroomStats): Boolean 

  def assignedTo(task: Task, host: String, port: Int)

  def assign(from: ActorRef, task: Task)

  def finalize(ticket: Ticket)

}

protected[master] class DefaultAssigner(jobManager: JobManager) 
      extends Assigner with CommonLog {

  override def dropRequest(stats: GroomStats, target: ActorRef) { }

  override def examine(jobManager: JobManager): Option[Ticket] = 
    jobManager.allowPassiveAssign && !jobManager.isEmpty(TaskAssign) match {
      case true => jobManager.headOf(TaskAssign)
      case false => None
    }

  override def validate(job: Job, stats: GroomStats): Boolean = {
    val current = job.getTaskCountFor(stats.hostPort)
    val allowed = stats.maxTasks
    (allowed >= (current + 1))
  } 

  override def assignedTo(task: Task, host: String, port: Int) = 
    task.assignedTo(host, port)

/*
  override def assign(ticket: Ticket, stats: GroomStats, target: ActorRef) =
    validate(ticket.job, stats) match { 
      case true => ticket.job.nextUnassignedTask match {
        case null => 
        case task@_ => {
          val (g1, t1) = beforeAssign(target, task)
          val (g2, t2) = internalAssign(g1, t1)
          afterAssign(g2, t2)
          finalize(ticket) match {
            case true =>
            case false =>
          }
        }
      }
      case false =>
    }
*/

  override def assign(from: ActorRef, task: Task) = task.getId.getId match {
    case id if 1 < id => from ! new Directive(Resume, task, "master") 
    case _ => from ! new Directive(Launch, task, "master")
  }

  override def finalize(ticket: Ticket) = if(ticket.job.allTasksAssigned) {
    LOG.debug("Tasks for job {} are all assigned!", ticket.job.getId)
    jobManager.moveToNextStage(ticket.job.getId) match { 
      case (true, _) => if(jobManager.update(ticket.newWith(ticket.job.
                           newWithRunningState))) 
        LOG.info("All tasks assigned, job {} is running!", ticket.job.getId)
      case _ => LOG.error("Unable to move job {} to next stage!", 
                          ticket.job.getId)
    } 
  }
 
}
