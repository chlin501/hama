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
import org.apache.hama.bsp.BSPJobID
import org.apache.hama.bsp.v2.Job
import org.apache.hama.fs.InitializeJob
import org.apache.hama.HamaConfiguration
import org.apache.hama.LocalService
import org.apache.hama.Request
import org.apache.hama.util.Curator
import scala.collection.immutable.Queue
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

final case object RequestInitJob

/**
 * Receive job submission from clients and put the job to the wait queue.
 */
class Receptionist(conf: HamaConfiguration) extends LocalService {

  type JobFile = String

  protected var waitQueue = Queue[Job]()

  /**
   * Job is stored before being completely initialized.
   */
  protected var storageQueue = Queue[(BSPJobID, JobFile)]()
 
  override def configuration: HamaConfiguration = conf

  override def name: String = "receptionist"

  def notifyJobSubmission = mediator ! Request("sched", JobSubmission)  

  override def afterMediatorUp {
    LOG.info("Mediator is up! Request to ask init job ...")
    request(self, RequestInitJob)
  }

  def request(to: ActorRef, message: Any) {
    import context.dispatcher
    context.system.scheduler.schedule(0.seconds, 2.seconds, to, message)
  }

  def askForInit(jobId: BSPJobID, jobFile: String) { 
    mediator ! Request("storage", InitializeJob(jobId, jobFile))
  }

  /**
   * BSPJobClient call submitJob(jobId, jobFile), where jobFile submitted is
   * the actual job.xml content.
   */
  def submitJob: Receive = {
    case Submit(jobId: BSPJobID, jobFile: String) => {
      LOG.info("Received job {} submitted from the client {}",
               jobId, sender.path.name) 
      storageQueue = storageQueue.enqueue((jobId, jobFile))
    }
  }

  /**
   * Ask Storage actor for initializing a job.
   */
  def requestInitJob: Receive = {
    case RequestInitJob => {
      if(!storageQueue.isEmpty) {
        val (tuple, rest) = storageQueue.dequeue
        askForInit(tuple._1, tuple._2) 
      } else LOG.info("Receptionist's storageQueue is empty!")
    }
  }

  /**
   * After a job is initialized, enqueue that job to waitQueue and notify
   * the scheduler.
   */
  def enqueue: Receive = {
    case Enqueue(job) => {
      waitQueue = waitQueue.enqueue(job)
      LOG.debug("Inside waitQueue: {}", waitQueue)
      notifyJobSubmission
    }
  }
  /**
   * Dispense a job to Scheduler.
   */
  def take: Receive = {
    case Take => {
      if(!waitQueue.isEmpty) {
        val (job, rest) = waitQueue.dequeue
        waitQueue = rest 
        LOG.info("Dispense a job {}. Now {} jobs left in wait queue.", 
                 job.getName, waitQueue.size)
        sender ! Dispense(job)
      } else LOG.warning("{} jobs in wait queue", waitQueue.size)
    }
  }

  override def receive = enqueue orElse requestInitJob orElse isServiceReady orElse serverIsUp orElse take orElse submitJob orElse unknown

}
