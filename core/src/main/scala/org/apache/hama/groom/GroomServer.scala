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

import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy.Escalate
import akka.actor.SupervisorStrategy.Restart
import akka.actor.SupervisorStrategy.Stop
import org.apache.hama.HamaConfiguration
import org.apache.hama.ServiceStateMachine
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

final class GroomServer(conf: HamaConfiguration) extends ServiceStateMachine {

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 3, withinTimeRange = 1 minute) {
      case _: NullPointerException     => Restart
      case _: IllegalArgumentException => Stop
      case _: Exception                => Escalate
    }

  override def configuration: HamaConfiguration = conf

  override def name: String = conf.get("bsp.groom.name", "groomServer") 

  override def initializeServices {
    create("taskManager", classOf[TaskManager]) 
    create("monitor", classOf[Monitor]) 
    create("registrator", classOf[Registrator]) 
  }

  override def receive = serviceStateListenerManagement orElse super.receive orElse unknown
}
