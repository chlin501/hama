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
package org.apache.hama.client

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import org.apache.hama.Event
import org.apache.hama.EventListener
import org.apache.hama.HamaConfiguration
import org.apache.hama.ProxyInfo
import org.apache.hama.RemoteService
import org.apache.hama.bsp.BSPJob
import org.apache.hama.bsp.BSPJobID
import org.apache.hama.conf.Setting
import org.apache.hama.master.Reject
import org.apache.hama.util.MasterDiscovery

final case object JobCompleteEvent extends Event

trait SubmitterMessage
final case object AskForJobId extends SubmitterMessage
final case class JobComplete(jobId: BSPJobID) extends SubmitterMessage
final case class NewJobID(id: BSPJobID) extends SubmitterMessage
final case class Submit(job: BSPJob) extends SubmitterMessage

object Submitter {

  private var client: Option[ActorRef] = None

  def startup(args: Array[String]) {
    val setting = Setting.client
    val system = ActorSystem(setting.info.getActorSystemName, setting.config) 
    client = Option(system.actorOf(Props(setting.main, setting), setting.name))
  }

  def simpleName(conf: HamaConfiguration): String = 
    conf.get("client.name", classOf[Submitter].getSimpleName) + "#" +
    scala.util.Random.nextInt  

  // TODO: business methods
  def submit(job: BSPJob): Boolean = client.map { ref => 
    ref ! Submit(job)
    true
  }.getOrElse(false)

  //def process(): Progress = { }  <- periodically check job process in master.

}

class Submitter(setting: Setting) extends RemoteService with MasterDiscovery 
                                                        with EventListener {

  protected var client: Option[ActorRef] = None

  protected var masterProxy: Option[ActorRef] = None

  protected var bspJob: Option[BSPJob] = None

  protected var bspJobId: Option[BSPJobID] = None

  override def setting(): Setting = setting

  override def initializeServices = retry("discover", 10, discover)

  override def afterLinked(target: String, proxy: ActorRef): Unit = {
    masterProxy = Option(proxy)
    bspJob.map { job => askForJobId }
  }

  protected def cache(job: BSPJob, from: ActorRef) = {
    client = Option(from)
    bspJob = Option(job)
  }

  protected def askForJobId() = bspJobId match {
    case Some(id) => LOG.warning("Job id alreay exists: {}!", id)
    case None => masterProxy.map { m => m ! AskForJobId }
  }

  protected def clientMsg: Receive = {
    case Submit(job) => masterProxy match {
      case Some(m) => {
        askForJobId 
        cache(job, sender)
      }
      case None => cache(job, sender)
    }
  }
  
  protected def masterMsg: Receive = {
    /**
     * Master creates a new job id for unsubmitted bsp job.
     */
    case NewJobID(id) => {
      bspJobId = Option(id)
      bspJob match { 
        case Some(job) => submitInternal(job, id) 
        case None => LOG.error("Unlikely but no bsp job submitted!")
      }
    }
    /**
     * Scheduler replies directly when the job is finished.
     */    
    case JobComplete(jobId) => cleanup
    case failure: Reject => { 
      LOG.error("Failing processing job because {}", failure.reason)
      cleanup
    }
  }

  // TODO: BSPJobClient.submitJobInternal
  protected def submitInternal(job: BSPJob, id: BSPJobID) {
  }

  protected def cleanup() = {
    LOG.info("Cleanup job related information and then shutdown sytem ...")
    client = None
    bspJobId = None 
    bspJob = None
    masterProxy = None
    shutdown
  }

  override def receive = clientMsg orElse masterMsg orElse unknown

}
