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
import akka.contrib.pattern.ReliableProxy
import org.apache.hama._
import org.apache.hama.sched.Scheduler
import org.apache.hama.monitor.DefaultMonitor
import scala.concurrent.duration._

class GroomServer(conf: HamaConfiguration) extends Director(conf) {

  val masterHost = conf.get("bsp.master.address", "localhost")
  val masterPort = conf.getInt("bsp.master.port", 40000)
  val masterSystem = conf.get("bsp.master.actor-system.name", "bspmaster")

  val masterPath = "akka.tcp://"+masterSystem+"@"+masterHost+":"+masterPort+
                   "/user/bspmaster"
  var lookupBSPMaster: Cancellable = _
  var bspmaster: ActorRef = _
 
  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 3, withinTimeRange = 1 minute) {
      case _: NullPointerException     => Restart
      case _: IllegalArgumentException => Stop
      case _: Exception                => Escalate
    }

  def lookupMaster() {
    context.system.actorSelection(masterPath) ! Identify(masterPath)
    import context.dispatcher
    lookupBSPMaster = 
      context.system.scheduler.scheduleOnce(3.seconds, self, ReceiveTimeout)
  }

  def initialize() {
    create("taskManager", classOf[GroomServer]) 
    create("monitor", classOf[DefaultMonitor]) 
    create("registrator", classOf[Registrator]) 
  }

  override def preStart() {
    initialize() 
  }

  def receive = {
    case ActorIdentity(`masterPath`, Some(master)) => {
      bspmaster = context.system.actorOf(Props(classOf[ReliableProxy],
                                               master,
                                               100.millis),
                                         "bspmaster")
      lookupBSPMaster.cancel
      LOG.info("BSP master {}:{} is ready.", masterHost, masterPort)
      services.values.foreach( service => {
        service ! Proxy(bspmaster)
      })
    }
    case ActorIdentity(`masterPath`, None) => {
      LOG.warning("Can't find master server at {}:{}.", masterHost, masterPort) 
    }
    case ReceiveTimeout => {
      LOG.info("Timeout! Lookup master again ...")
      lookupMaster
    }
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
