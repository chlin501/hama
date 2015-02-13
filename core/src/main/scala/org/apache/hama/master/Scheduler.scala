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
import org.apache.hama.HamaConfiguration
import org.apache.hama.SystemInfo
import org.apache.hama.master.Directive.Action._
import org.apache.hama.logging.CommonLog

object Scheduler {

  def create(conf: HamaConfiguration): Scheduler = {
    val default = classOf[DefaultScheduler]
    val clazz = conf.getClass("bsp.sched.class", default, classOf[Scheduler])
    ReflectionUtils.newInstance(clazz, conf)
  }

}

trait Scheduler {

  def receive(ticket: Ticket)

  def examine(ticket: Ticket): Boolean 

  def findGroomsFor(ticket: Ticket, f: (Array[SystemInfo]) => Unit)

  def found(targets: Array[ActorRef], 
            v: (Array[SystemInfo], Array[ActorRef]) => Boolean,
            b: (ActorRef, Task) => (ActorRef, Task),
            s: (ActorRef, Task) => (ActorRef, Task),
            a: (ActorRef, Task) => Unit,
            f: (Ticket) => Unit) 

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

  override def findGroomsFor(ticket: Ticket, f: (Array[SystemInfo]) => Unit) { 
    val infos = ticket.job.targetInfos 
    LOG.debug("{} requests target grooms {} for scheduling!", 
              getClass.getSimpleName, infos.mkString(","))
    f(infos) 
  }

  override def found(targets: Array[ActorRef], 
                     v: (Array[SystemInfo], Array[ActorRef]) => Boolean,
                     b: (ActorRef, Task) => (ActorRef, Task),
                     s: (ActorRef, Task) => (ActorRef, Task),
                     a: (ActorRef, Task) => Unit,
                     f: (Ticket) => Unit) = { 
    if(!jobManager.isEmpty(TaskAssign)) jobManager.headOf(TaskAssign).map { t =>
      v(t.job.targetInfos, targets) match {
        case true => targets.foreach { groom => t.job.nextUnassignedTask match {
          case null =>
          case task@_ => {
            val (host, port) = targetHostPort(groom)
            task.scheduleTo(host, port)
            val (g1, t1) = b(groom, task)
            val (g2, t2) = s(g1, t1) 
            a(g2, t2) 
            f(t)
          }
        }}
        case false => // TODO: mark job failed, notify clinet, move to finished queue
      }
    } else LOG.error("No job exists at TaskAssign stage when active " + 
                             "grooms found!")
    jobManager.markScheduleFinished
  }

  protected def targetHostPort(ref: ActorRef): (String, Int) = {
    val host = ref.path.address.host.getOrElse("")
    val port = ref.path.address.port.getOrElse(50000)
    (host, port)
  }

  protected def beforeSchedule(a: ActorRef, t: Task): (ActorRef, Task) = (a, t) 
 
  protected def scheduleInternal(ref: ActorRef, t: Task): (ActorRef, Task) = {
    t.getId.getId match {
      case id if 1 < id => ref ! new Directive(Resume, t, "master")
      case _ => ref ! new Directive(Launch, t, "master")
    }
    (ref, t)
  }

  protected def afterSchedule(a: ActorRef, t: Task) { }


}
