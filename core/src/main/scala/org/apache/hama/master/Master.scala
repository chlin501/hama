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
import scala.concurrent.duration._

class Master(conf: HamaConfiguration) extends ServiceStateMachine {

  override def configuration: HamaConfiguration = conf

  override def name: String = configuration.get("bsp.master.name", "bspmaster")
 
  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 3, withinTimeRange = 1 minute) {
      case _: NullPointerException     => Restart
      case _: IllegalArgumentException => Stop
      case _: Exception                => Escalate
    }

  override def initializeServices {
    create("groomManager", classOf[GroomManager]) 
    create("monitor", classOf[Monitor]) 
    create("sched", classOf[Scheduler]) 
    create("receptionist", classOf[Receptionist]) 
  }

  override def receive = {
    serviceStateListenerManagement orElse unknown 
  }
}
