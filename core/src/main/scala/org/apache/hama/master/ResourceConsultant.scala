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
import akka.routing._
import org.apache.hama._
import scala.concurrent.duration._

class ResourceConsultant(conf: HamaConfiguration) extends LocalService {

  override def configuration: HamaConfiguration = conf

  override def name: String = "resourceConsultant"

  override def initializeServices { }

  def checkResource: Receive = {
    case CheckResource(job) => {
      val resource = Resource(job, 
                              Seq("monitor", "groomTasksTracker", 
                                  "monitor", "sysMetricsTracker"),
                              Set.empty[GroomAvailable])
      mediator ! Request("groomManager", resource) 
      // find groom alive and its spec // GroomManager
      // find free slots in grooms // GroomTracker
      // find lowest sys stats // SysMetricsTracker 
      // Resource(job, Groom with free slots)
    }
  } 
 
  override def receive = isServiceReady orElse serverIsUp orElse unknown

}
