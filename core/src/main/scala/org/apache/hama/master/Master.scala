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
import akka.actor.SupervisorStrategy._
import org.apache.hama._
import org.apache.hama.sched.Scheduler
import org.apache.hama.monitor.DefaultMonitor
import scala.concurrent.duration._

class Master(conf: HamaConfiguration) extends Director(conf) {
 
  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 3, withinTimeRange = 1 minute) {
      case _: NullPointerException     => Restart
      case _: IllegalArgumentException => Stop
      case _: Exception                => Escalate
    }

  def initialize() {
    create("groomManager", classOf[GroomManager]) 
    create("monitor", classOf[DefaultMonitor]) 
    create("sched", classOf[Scheduler]) 
    create("receptionist", classOf[Receptionist]) 
  }

  override def preStart() {
    initialize() 
  }

  override def receive = {
    ({case Ready => {
      if(4 != services.size) {
        LOG.info("Currently {} services are ready.", services.size)
        sender ! Ack("no") // perhaps skip
      } else {
        sender ! Ack("yes")
      }     
    }}: Receive) orElse ack orElse unknown 
  } 
}
