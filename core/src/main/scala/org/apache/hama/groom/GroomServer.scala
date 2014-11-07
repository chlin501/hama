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
package org.apache.hama.groom

import akka.actor.ActorSystem
import akka.actor.Props
import akka.cluster.Member
import org.apache.hama.HamaConfiguration
import org.apache.hama.LocalService
import org.apache.hama.conf.Setting
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

object GroomServer {

  def main(args: Array[String]) {
    val groom = Setting.groom
    // TODO: lookup master and pass that info to groom server
    val sys = ActorSystem(groom.info.getActorSystemName, groom.config)
    sys.actorOf(Props(classOf[GroomServer], groom), groom.name)
  }
}

final class GroomServer(setting: Setting) extends LocalService with MembershipParticipant { // ServiceStateMachine {

  // TODO: create system info 
  //       pass sys info to task manager
  //       move task manager register to groom server (let groom register)
  //   

  override def initializeServices {
    // lookup zk and retrieve master sys info
    //join(Vector(master sys info))
    //subscribe(self)
    val monitor = getOrCreate("monitor", classOf[Reporter], setting.hama) 
    getOrCreate("taskManager", classOf[TaskManager], setting.hama, monitor) 
  }

  override def stopServices = unsubscribe(self)

  override def register(member: Member) {
    // TODO: lookup master 
    //       register member data e.g. master ! GroomRegistration
  }

  override def receive = membership orElse unknown
}
