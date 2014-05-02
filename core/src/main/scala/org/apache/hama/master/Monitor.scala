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

import org.apache.hama.HamaConfiguration
import org.apache.hama.Load
import org.apache.hama.LocalService
import org.apache.hama.master.monitor.JobTasksTracker
import org.apache.hama.master.monitor.GroomTasksTracker
import org.apache.hama.master.monitor.SysMetricsTracker

// TODO: rename to Auditor?
class Monitor(conf: HamaConfiguration) extends LocalService {

  override def configuration: HamaConfiguration = conf

  override def name: String = "monitor"

  override def initializeServices {
    create("jobTasksTracker", classOf[JobTasksTracker])
    create("groomTasksTracker", classOf[GroomTasksTracker])
    create("sysMetricsTracker", classOf[SysMetricsTracker])
  }

  def loadPlugin: Receive = {  
    case Load => {
      LOG.debug("Receiveing {} plugin.", sender.path.name)
      cacheService(sender) 
    }
  }

  override def receive = forward orElse areSubServicesReady orElse serverIsUp orElse loadPlugin orElse unknown

}
