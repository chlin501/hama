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
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.hama.HamaConfiguration
import org.apache.hama.ServiceStateMachine
import org.apache.hama.util.Curator
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

final private[hama] case class Id(value: String)

/*
object BSPMaster {

  val defaultMasterId = 
    new SimpleDateFormat("yyyyMMddHHmm").format(new Date())

  val masterPath = (name: String) => "/bsp/masters/%s/id".format(name)
 
}
*/

class BSPMaster(conf: HamaConfiguration) extends ServiceStateMachine 
                                         with Curator {

  //import BSPMaster._

  //private var identifier: String = _

  override def configuration: HamaConfiguration = conf
 
/*
  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 3, withinTimeRange = 1 minute) {
      case _: NullPointerException     => Restart
      case _: IllegalArgumentException => Stop
      case _: Exception                => Restart
    }

  def createMasterId: String = getOrElse(masterPath(name), defaultMasterId)
*/

  override def initializeServices {
    //initializeCurator(configuration)
    //identifier = createMasterId
    //LOG.info("BSPMaster identifier is {}", identifier)
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
