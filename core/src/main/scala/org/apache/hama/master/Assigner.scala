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
    val cls = conf.getClass("master.assigner.class", default, classOf[Assigner])
    Try(cls.getConstructor(classOf[JobManager]).newInstance(jobManager)) match {
      case Success(instance) => instance
      case Failure(cause) => throw cause 
    }
  }

}

trait Assigner {

  def examine(jobManager: JobManager): Option[Ticket] 

  def validate(ticket: Ticket, stats: GroomStats): Boolean 

  def assign(ticket: Ticket, stats: GroomStats, target: ActorRef)

}

protected[master] class DefaultAssigner(jobManager: JobManager) 
      extends Assigner with CommonLog {

  override def examine(jobManager: JobManager): Option[Ticket] = 
    jobManager.allowPassiveAssign && !jobManager.isEmpty(TaskAssign) match {
      case true => jobManager.headOf(TaskAssign)
      case false => None
    }

  override def validate(ticket: Ticket, stats: GroomStats): Boolean = {
    val current = ticket.job.getTaskCountFor(stats.hostPort)
    val allowed = stats.maxTasks
    (allowed >= (current + 1))
  } 

  protected[master] def before(to: ActorRef, task: Task): (ActorRef, Task) = {
    val (host, port) = resolve(to)
    task.assignedTo(host, port)
    (to, task)
  }

  protected[master] def resolve(ref: ActorRef): (String, Int) = {
    val host = ref.path.address.host.getOrElse("localhost")
    val port = ref.path.address.port.getOrElse(50000)
    (host, port)
  }

  override def assign(ticket: Ticket, stats: GroomStats, target: ActorRef) =
    ticket.job.nextUnassignedTask match {
      case null => 
      case task@_ => {
        val (g1, t1) = before(target, task)
        val (g2, t2) = internal(g1, t1)
        after(g2, t2)
        finalize(ticket) 
      }
    }

  protected[master] def internal(ref: ActorRef, t: Task): (ActorRef, Task) = {
    t.getId.getId match {
      case id if 1 < id => ref ! new Directive(Resume, t, "master") 
      case _ => ref ! new Directive(Launch, t, "master")
    }
    (ref, t)
  }

  protected[master] def after(ref: ActorRef, task: Task) { }

  protected[master] def finalize(ticket: Ticket) = 
    if(ticket.job.allTasksAssigned) {
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
