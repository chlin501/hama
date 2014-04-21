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
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy.Restart
import akka.actor.SupervisorStrategy.Stop
import org.apache.hama.HamaConfiguration
import org.apache.hama.Request
import org.apache.hama.ServiceStateMachine
import org.apache.hama.fs.Storage
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

final private[hama] case class Id(value: String)

class Master(conf: HamaConfiguration) extends ServiceStateMachine {

  private var identifier: String = _

  override def configuration: HamaConfiguration = conf

  override def name: String = 
    configuration.get("bsp.master.name", "bspmaster")
 
  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 3, withinTimeRange = 1 minute) {
      case _: NullPointerException     => Restart
      case _: IllegalArgumentException => Stop
      case _: Exception                => Restart
    }

  override def initializeServices {
    create("masterConfigurator", 
           classOf[MasterConfigurator]).withCondition("masterConfigurator")
    create("storage", classOf[Storage]) 
    create("receptionist", classOf[Receptionist]) 
    create("groomManager", classOf[GroomManager]) 
    create("monitor", classOf[Monitor]) 
    create("sched", classOf[Scheduler]) 
  }

  override def afterLoaded(service: ActorRef) {
    val serviceName = service.path.name
    serviceName match {
      case "receptionist" => 
      case "groomManager" => 
      case "monitor" => 
      case "sched" =>  
      case "curator" => service ! GetMasterId 
      case "storage" => 
      case _ => LOG.warning("Unknown service {} ", serviceName)
    }
  }
 
  def masterId: Receive = {
    case MasterId(value) => {
      val id = value.getOrElse(null)
      LOG.info("Obtained MasterId is {}", id)
      if(null != id) identifier = id 
      releaseCondition("curator")
    } 
  }

  def forward: Receive = {
    case Request(service, message) => { 
      services.find(p => service.equals(p.path.name)) match {
        case Some(found) => found forward message 
        case None => 
          LOG.warning("Can't forward message because {} not found! Services"+ 
                      " available: {}.", service, services.mkString(", "))
      }
    }
  }

  override def receive = forward orElse serviceStateListenerManagement orElse masterId orElse super.receive orElse unknown 
  
}
