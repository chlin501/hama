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

import akka.actor.ActorSystem
import akka.actor.Props
import org.apache.hama.ProxyInfo
import org.apache.hama.RemoteService
import org.apache.hama.EventListener
import org.apache.hama.HamaConfiguration
import org.apache.hama.bsp.BSPJobID
import org.apache.hama.conf.Setting
import org.apache.hama.util.MasterDiscovery

trait SubmitterMessage
final case class JobCompleted(jobId: BSPJobID) extends SubmitterMessage

object Submitter {

  def main(args: Array[String]) {
    val client = Setting.client
    val system = ActorSystem(client.info.getActorSystemName, client.config) 
    system.actorOf(Props(client.main, client), client.name)
  }

  def simpleName(conf: HamaConfiguration): String = 
    conf.get("client.name", classOf[Submitter].getSimpleName) + "#" +
    scala.util.Random.nextInt  

  // TODO: business methods
  //def submit(job: BPSJob): Boolean = {
  //}
  //def process(): Progress = { }  <- periodically check job process in master.
}

class Submitter(setting: Setting) extends RemoteService with MasterDiscovery 
                                                        with EventListener {

  override def initializeServices = retry("discover", 10, discover)

  protected def events: Receive = {
    case JobCompleted(jobId) => // TODO: subscribe to JobFinished event
  }

  override def receive = events orElse unknown
}
