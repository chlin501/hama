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
import org.apache.hama.bsp.v2.Job
import org.apache.hama.master._
import scala.collection.immutable.Queue

/**
 * Receive job submission from clients and put the job to the wait queue.
 */
class Receptionist(conf: HamaConfiguration) extends Service(conf) with Listeners {

  private[this] var waitQueue = Queue[Job]()

  def name: String = "receptionist"

  override def receive = {
    //case JobDispatchAck(job) => { 
      // remove the corresponded job from the wait queue.
    //}
    case Take => {
      var result: Option[Job] = None
      if(0 < waitQueue.size) {
        val (head, rest) = waitQueue.dequeue
        //waitQueue = rest // won't remove the job in queue, wait for sched ack
        result = Some(head)
        LOG.info("New job {}, with {} jobs left in queue.", 
                 head.getName, waitQueue.size)
      } 
      sender ! TakeResult(result)
    }
    ({case Submit(job: Job) => {
      LOG.info("Client submit job ..."+job.getName) 
      waitQueue = waitQueue.enqueue(job)
      gossip(NewJobNotification)
    }}: Receive) orElse ready orElse listenerManagement orElse unknown
  } 
}
