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

import akka.actor._
import akka.routing._
import org.apache.hama._
import scala.concurrent.duration._

class Scheduler(conf: HamaConfiguration) extends Service(conf) {

  val queuePath = "/usr/receptionist"
  var cancelQueueWhenReady: Cancellable = _
  var queue: ActorRef = _

  // var job_resource = Map.empty[String, Resource]

  override def name: String = "sched"

  private def subscribe(path: String): Cancellable = {
    context.system.actorSelection(path) ! Identify(path)
    import context.dispatcher
    val cancellable = 
      context.system.scheduler.scheduleOnce(3.seconds, self, Timeout(path))
    cancellable
  } 

  //private def find = {
  //}

  override def initialize = { 
    cancelQueueWhenReady = subscribe(queuePath)
    // command dispatcher
    // resouce consultant
  }

  private def isQueueReady: Receive = {
    case ActorIdentity(`queuePath`, Some(receptionist)) => {
      queue = receptionist
      queue ! Listen(self)
      cancelQueueWhenReady.cancel 
    }
    case ActorIdentity(`queuePath`, None) => { 
      LOG.info("Queue at {} not yet available.", queuePath) 
    }
    case Timeout(path) => {
      cancelQueueWhenReady = subscribe(path)
    }
  }
 
  override def receive = {
    //case TakeResult(job) => {
      // retrieve slots info with corresponded job from job_resource
      // send commands to command dispatcher
      // command dispatcher ! (job, resource) 
      // command dispatcher ensures command will be successfully dispatched
      // e.g. resend 3 times if any failure e.g. i/o 
    //}
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
    ({case NewJobNotification => {
      // check with resource consultant if free slots available
      // resourceConsultant ! jobName 
    }}: Receive) orElse isQueueReady orElse ready orElse unknown
  }

}
