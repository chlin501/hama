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

import akka.actor._
import akka.actor.SupervisorStrategy._
import org.apache.hama._
import scala.concurrent.duration._

final class GroomServer(conf: HamaConfiguration) extends Service(conf) {

/*
  private[groom] val masterInfo =
    ProxyInfo(conf.get("bsp.msater.name", "bspmaster"),
              conf.get("bsp.master.actor-system.name", "MasterSystem"),
              conf.get("bsp.master.address", "localhost"),
              conf.getInt("bsp.master.port", 40000))
*/

 
  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 3, withinTimeRange = 1 minute) {
      case _: NullPointerException     => Restart
      case _: IllegalArgumentException => Stop
      case _: Exception                => Escalate
    }

  override def name: String = conf.get("bsp.groom-server.name", "groomServer") 

  override def initialize() {
    create("taskManager", classOf[TaskManager]) 
    create("monitor", classOf[Monitor]) 
    create("registrator", classOf[Registrator]) 
  }

  def receive = {
    ({case Ready => { // reply to external check if service is ready.
      if(serviceCount != services.size) {
        LOG.info("Currently only {} services are ready.", services.size)
      } else {
        sender ! Ack("yes")
      }     
    }}: Receive) orElse ack orElse unknown 
  } 
}
