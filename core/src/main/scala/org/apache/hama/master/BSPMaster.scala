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
import akka.actor.Props
import org.apache.hama.HamaConfiguration
import org.apache.hama.conf.Setting
import org.apache.hama.ServiceStateMachine
import org.apache.hama.util.Curator
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

object BSPMaster {

  def main(args: Array[String]) {
    val master = Setting.master
    //TODO: post seed node info to zk
    val sys = ActorSystem(master.info.getActorSystemName, master.config)
    sys.actorOf(Props(classOf[BSPMaster], master), 
                master.name)
  }
 
}

class BSPMaster(setting: Setting) extends ServiceStateMachine {

  import BSPMaster._

  override def configuration: HamaConfiguration = setting.hama

  override def initializeServices {
    val receptionist = getOrCreate("receptionist", classOf[Receptionist], 
                                   configuration) 
    getOrCreate("monitor", classOf[Monitor], configuration) 
    val sched = getOrCreate("sched", classOf[Scheduler], configuration, 
                            receptionist) 
    getOrCreate("groomManager", classOf[Scheduler], configuration, 
                receptionist, sched) 
  }

  override def receive = serviceStateListenerManagement orElse super.receive orElse unknown
  
}
