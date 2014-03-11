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
import org.apache.hama._
import org.apache.hama.master.monitor._

class Monitor(conf: HamaConfiguration) extends Service {

  override def configuration: HamaConfiguration = conf

  override def name: String = "monitor"

  override def initializeServices {
    create("jobTasksTracker", classOf[JobTasksTracker])
  }

  /**
   * Monitor needs to load sub services so it requires to override 
   * isServiceReady function.
   */
  override def isServiceReady: Receive = {
    case IsServiceReady => {
      if(servicesCount == services.size) { 
        sender ! Load(name, self)
      } else LOG.info("{} are available.", services.keys.mkString(", "))
    }
  }

  def loadPlugin: Receive = {  
    case Load(name, ref) => { 
      cacheService(name, ref) 
    }
  }

  override def receive = {
    isServiceReady orElse loadPlugin orElse unknown
  } 
}