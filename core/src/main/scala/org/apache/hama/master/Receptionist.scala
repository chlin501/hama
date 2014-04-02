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

import akka.routing._
import org.apache.hama._
import org.apache.hama.bsp.BSPJobID
import org.apache.hama.bsp.v2.Job
import org.apache.hama.bsp.v2.TaskTable
import org.apache.hama.master._
import scala.collection.immutable.Queue

/**
 * Receive job submission from clients and put the job to the wait queue.
 */
class Receptionist(conf: HamaConfiguration) extends LocalService {

  private[this] var waitQueue = Queue[Job]()
 
  override def configuration: HamaConfiguration = conf

  override def name: String = "receptionist"

  def initJob(jobId: BSPJobID, xml: String): Job = 
    new Job.Builder().setId(jobId).
                      setConf(conf).
                      setJobXml(xml).
                      setTaskTable(new TaskTable(jobId, conf)).
                      build()

  /**
   * BSPJobClient call submitJob(jobId, jobFile)
   */
  def submitJob: Receive = {
    case Submit(jobId: BSPJobID, xml: String) => {
      LOG.info("Received job {} submitted from the client {}",
               jobId, sender.path.name) 
      val job = initJob(jobId, xml)
      waitQueue = waitQueue.enqueue(job)
      mediator ! Request("sched", JobSubmission)  
    }
  }

  /**
   * Dispense a job to Scheduler.
   */
  def take: Receive = {
    case Take => {
      if(0 < waitQueue.size) {
        val (job, rest) = waitQueue.dequeue
        waitQueue = rest 
        LOG.info("Dispense a job {}. Now {} jobs left in wait queue.", 
                 job.getName, waitQueue.size)
        sender ! Dispense(job)
      } else LOG.warning("{} jobs in wait queue", waitQueue.size)
    }
  }

  override def receive = isServiceReady orElse serverIsUp orElse take orElse submitJob orElse unknown

}
