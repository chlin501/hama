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

  private var taskAssignQueue = Queue[Job]()
  private var processingQueue = Queue[Job]()

  override def configuration: HamaConfiguration = conf

  override def name: String = "sched"

  override def initializeServices { 
    //cancelQueueWhenReady = subscribe(queuePath)
    // command dispatcher
    // resouce consultant
  }

/*
    //case CommandStatus(jobName, status) => {
      // if(fail) 
      //   ?? Stop or Escalate ??
      // else
      //   ack to receptionist for removing corresponded job
    //}
    // resource consultant won't reply until resource avail
    // so resource consultant itself will wait if it finds not 
    // enough resource. 
    // then once a job finishes (ie resource is freed from grooms by listening
    // to jobTasksTracker), associate wait job with resource and reply to sched
    //case ResurouceAvailabe(jobName, resource) => {
      // cache job with free slots information e.g job_resource
      // queue ! Take
    //} 
*/

  /**
   * GroomServer request for assigning a task.
   * TODO: check if a job's tasks need to be assigned to a reserved 
   *       GroomServer. if yes, skip the this request, go with assignTask 
   *       instead
   */
  def requestTask: Receive = {
    case RequestTask => {
      val (job, rest) = taskAssignQueue.dequeue
      unassignedTask(job) match {
        case Some(task) => {
          LOG.info("Assign the task {} to {}", task.getId, sender.path.name)
          task.changeToAssigned
          sender ! new Directive(Launch, task,  
                                 conf.get("bsp.master.name", "bspmaster"))  
        }
        case None => {
          taskAssignQueue = rest
          processingQueue.enqueue(job)
        }
      }
    }
  }

  def unassignedTask(job: Job): Option[Task] = {
    val task = job.nextUnassignedTask;
    if(null != task) Some(task) else None
  }

  def reschedTasks: Receive = {
    case RescheduleTasks(spec) => {
       LOG.info("Failed GroomServer having GroomServerSpec "+spec)
    }
  }

  def jobSubmission: Receive = {
    case JobSubmission => {
      mediator ! Request("receptionist", Take)
    }
  }

  /**
   * Positive assign tasks.
   */ 
  def assignTask() { }

  def takeResult: Receive = {
    case TakeResult(job) => { 
      taskAssignQueue = taskAssignQueue.enqueue(job)
      assignTask
    }
  }

  override def receive = isServiceReady orElse serverIsUp orElse reschedTasks orElse jobSubmission orElse unknown

}
