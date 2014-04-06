/**
 * Licensed to the Apache Software Foundation (ASF) under one * or more contributor license agreements.  See the NOTICE file
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

import akka.actor._
import akka.routing._
import org.apache.hama._
import org.apache.hama.groom._
import org.apache.hama.bsp.v2.Job
import org.apache.hama.bsp.v2.Task
import org.apache.hama.bsp.v2.GroomServerSpec
import org.apache.hama.master.Directive.Action._
import scala.concurrent.duration._
import scala.collection.immutable.Queue

class Scheduler(conf: HamaConfiguration) extends LocalService 
                                         with RemoteService {

  type TaskAssignQueue = Queue[Job]
  type ProcessingQueue = Queue[Job]

  val taskManagerInfo = new ProxyInfo.Builder().withConfiguration(conf).
                                                withActorName("taskManager").
                                                appendRootPath("groomServer").
                                                appendChildPath("taskManager").
                                                buildProxyAtGroom

  /**
   * A queue that holds jobs with tasks left unassigning to GroomServers.
   */
  private var taskAssignQueue = Queue[Job]()

  /**
   * A queue that holds jobs having all tasks assigned to GroomServers.
   */
  private var processingQueue = Queue[Job]()

  override def configuration: HamaConfiguration = conf

  override def name: String = "sched"

  /**
   * GroomServer request for assigning a task.
   */
  def requestTask: Receive = {
    case RequestTask(groomServerName) => {
      val (from, to) = 
        assign(groomServerName, taskAssignQueue, sender, dispatch) 
      taskAssignQueue = from
      processingQueue = to
    }
  }

  /**
   * Assign a task to a particular GroomServer by delegating that task to 
   * dispatch function, which normally uses actor ! message.
   */
  // TODO: move to a Trait.
  def assign(fromGroom: String, fromQueue: TaskAssignQueue, 
             fromActor: ActorRef, d: (ActorRef, Task) => Unit): 
      (TaskAssignQueue, ProcessingQueue) = {
    val (job, rest) = fromQueue.dequeue
    var from = Queue[Job]()
    val to = bookThenDispatch(job, fromActor, fromGroom, d) 
    if(!to.isEmpty) from = rest
    (from, to)
  }

  private def dispatch(from: ActorRef, task: Task) {
    from ! new Directive(Launch, task,  
                           conf.get("bsp.master.name", "bspmaster"))  
  }

  /**
   * Dispense next unassigned task. None indiecates all tasks are assigned.
   */
  def unassignedTask(job: Job): Option[Task] = {
    val task = job.nextUnassignedTask;
    if(null != task) Some(task) else None
  }
  
 /**
  * Rescheduling tasks when a GroomServer goes offline.
  */
  def reschedTasks: Receive = {
    case RescheduleTasks(spec) => {
       LOG.info("Failed GroomServer having GroomServerSpec "+spec)
       // TODO:  not yet implemented
    }
  }

  /**
   * Once receiving job submission notification, Scheduler will request pulling 
   * a job from Receptionist.waitQueue for scheduling job's tasks.
   * The request result will be replied with Dispense message.
   */
  def jobSubmission: Receive = {
    case JobSubmission => mediator ! Request("receptionist", Take)
  }

  def bookThenDispatch(job: Job, targetActor: ActorRef,  
                       targetGroomServer: String, 
                       d: (ActorRef, Task) => Unit): ProcessingQueue = {
    var to = Queue[Job]()
    unassignedTask(job) match {
      case Some(task) => {
        task.markWithTarget(targetGroomServer) 
        d(targetActor, task)
      }
      case None => { 
        to.enqueue(job) 
      }
    }
    to
  }

  /**
   * Positive schedule tasks.
   * Actual function that acitively schedules tasks to GroomServers.
   */ 
  def schedule(fromQueue: TaskAssignQueue): 
      (TaskAssignQueue, ProcessingQueue) = { 
    val (job, rest) = fromQueue.dequeue
    val groomServers = job.getTargets  
    var from = Queue[Job](); var to = Queue[Job]()
    groomServers.foreach( groomName => {
      LOG.info("Retrieve GroomServer taskManager {}", groomName)
      proxies.find(p => p.path.name.equals(groomName)) match {
        case Some(taskManagerActor) => {
          to = bookThenDispatch(job, taskManagerActor, groomName, dispatch)
        }
        case None => 
          LOG.warning("Can't schedule tasks for taskManager {} not found.", 
                      groomName)
      }
    })
    if(!to.isEmpty) from = rest 
    (from, to)
  }

  /**
   * Move a job to a specific queue pending for further processing, either
   * passive or positive.
   */
  def dispense: Receive = {
    case Dispense(job) => { 
      taskAssignQueue = taskAssignQueue.enqueue(job)
      if(null != job.getTargets && 0 < job.getTargets.length) { // active
        val (from, to) = schedule(taskAssignQueue)  
        taskAssignQueue = from
        processingQueue = to
      }
    }
  }

  def lookupTaskManager(spec: GroomServerSpec) {
    LOG.info("Lookup {} at {}", spec.getName, taskManagerInfo.getPath)
    lookup(spec.getName, taskManagerInfo.getPath)
  }

  def locate: Receive = {
    case Locate(spec) => {
      lookupTaskManager(spec)
    }
  }

  override def receive = locate orElse isServiceReady orElse serverIsUp orElse reschedTasks orElse jobSubmission orElse dispense orElse requestTask orElse isProxyReady orElse timeout orElse unknown

}
