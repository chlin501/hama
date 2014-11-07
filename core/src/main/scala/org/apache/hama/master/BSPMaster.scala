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
import akka.actor.ActorSystem
import akka.actor.Address
import akka.actor.Props
import org.apache.hama.LocalService
import org.apache.hama.Registrator
import org.apache.hama.SystemInfo
import org.apache.hama.conf.Setting
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration
import scala.collection.immutable.IndexedSeq
import scala.collection.immutable.Vector

object BSPMaster {

  def main(args: Array[String]) {
    val master = Setting.master
    val registrator = Registrator(master)
    registrator.register
    val sys = ActorSystem(master.info.getActorSystemName, master.config)
    sys.actorOf(Props(classOf[BSPMaster], master), master.name)
  }
 
}

// TODO: member management trait for harness groom registration, leave, etc.
//       BSPMaster extends member management trait
//       refactor FSM (perhaps remove it)
class BSPMaster(setting: Setting) extends MembershipDirector { //ServiceStateMachine {

  import BSPMaster._

  override def initializeServices {
    join(seedNodes)
    subscribe(self)
    val receptionist = getOrCreate("receptionist", classOf[Receptionist], 
                                   setting.hama) 
    getOrCreate("monitor", classOf[Monitor], setting.hama) 
    val sched = getOrCreate("sched", classOf[Scheduler], setting.hama, 
                            receptionist) 
    getOrCreate("groomManager", classOf[Scheduler], setting.hama, 
                receptionist, sched) 
    // TODO: change master state
  }

  override def stopServices = unsubscribe(self) 

  def seedNodes(): IndexedSeq[SystemInfo] = Vector(setting.info)

  override def receive = membership orElse unknown
  
}
