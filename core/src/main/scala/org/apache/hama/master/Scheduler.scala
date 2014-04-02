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
import org.apache.hama.master.Directive.Action._
import scala.concurrent.duration._
import scala.collection.immutable.Queue

class Scheduler(conf: HamaConfiguration) extends LocalService {

  type TaskAssignQueue = Queue[Job]
  type ProcessingQueue = Queue[Job]

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
    // TODO: check if a job's tasks need to be assigned to a reserved 
    //       GroomServer. if yes, skip this request, go with assignTask()
    //       instead
    case RequestTask => {
      val (from, to) = assign(taskAssignQueue, processingQueue, dispatch) 
      taskAssignQueue = from
      processingQueue = to
    }
  }

  /**
   * Assign a task to a particular GroomServer by delegating that task to 
   * dispatch function, which normally uses actor ! message.
   * 
   * @param fromQueue denotes taskAssignQueue.
   * @param toQueue denotes processingQueue.
   */
  def assign(fromQueue: TaskAssignQueue, toQueue: ProcessingQueue, 
             dispatch: (ActorRef, Task) => Unit): 
      (TaskAssignQueue, ProcessingQueue) = {
    // TODO: move to a Trait.
    val (job, rest) = fromQueue.dequeue

    var _fromQueue = Queue[Job](); var _toQueue = Queue[Job]()
    unassignedTask(job) match {
      case Some(task) => {
        LOG.info("Assign the task {} to {}", task.getId, sender.path.name)
        task.changeToAssigned
        dispatch(sender, task)
      }
      case None => {
        _fromQueue = rest
        _toQueue.enqueue(job)
      }
    }
    (_fromQueue, _toQueue)
  }

  private def dispatch(from: ActorRef, task: Task) {
    from ! new Directive(Launch, task,  
                           conf.get("bsp.master.name", "bspmaster"))  
  }

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

  /**
   * Positive schedule tasks.
   * Actual function that schedules tasks to GroomServers.
   */ 
  def schedule(job: Job, from: TaskAssignQueue, to: ProcessingQueue):
      (TaskAssignQueue, ProcessingQueue) = { 
    // TODO: not yet implemented.
    (from, to)
  }

  /**
   * Move a job to a specific queue pending for further processing, either
   * passive or positive.
   */
  def dispense: Receive = {
    case Dispense(job) => { 
/*
      val (from, to) = preSchedule(job, taskAssignQueue, processingQueue)
      val (_from, _to) = schedule(job, from, to)
      postSchedule(job, _from, _to)
*/
    }
  }

  def postSchedule(job: Job, from: TaskAssignQueue, to: ProcessingQueue) {
    taskAssignQueue = from 
    processingQueue = to
  }

  /**
   * The function that would schedule tasks to particular GroomServers.
   */
  def preSchedule(job: Job, from: TaskAssignQueue, to: ProcessingQueue): 
      (TaskAssignQueue, ProcessingQueue) = {
    val _from = from.enqueue(job)
    (_from, to)
  }

  override def receive = isServiceReady orElse serverIsUp orElse reschedTasks orElse jobSubmission orElse dispense orElse requestTask orElse unknown

}
