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
package org.apache.hama.monitor

import akka.actor._
import akka.event._

import org.apache.hama.HamaConfiguration
import org.apache.hama.master._

class DefaultMonitor(conf: HamaConfiguration) extends Actor {

  val LOG = Logging(context.system, this)

  var services = Map.empty[String, ActorRef]

  def initialize() {
    val jobTasksTracker = 
      context.actorOf(Props(classOf[JobTasksTracker], conf), 
                      "job-tasks-tracker")
    jobTasksTracker ! Ready
  }

  override def preStart() {
    initialize()
  }

  def receive = {
    case Ready => {
      if(1 == services.size) {
        sender ! Ack("monitor")
      } else LOG.info("Only {} are available.", services.keys.mkString(", "))
    } 
    case Ack(name) => {
      services ++= Map(name -> sender)
      context.watch(sender)
      LOG.info("{} is loaded", name)
    }
    case _ => {
      LOG.warning("Unknown monitor message.")
    }

  }


}
