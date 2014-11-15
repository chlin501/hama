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

import akka.actor.ActorRef
import org.apache.hama.LocalService
import org.apache.hama.conf.Setting
import org.apache.hama.monitor.groom.TasksCollector
import org.apache.hama.monitor.groom.GroomCollector
import org.apache.hama.monitor.groom.SysMetricsCollector
import org.apache.hama.monitor.Report
import org.apache.hama.util.Curator

class Reporter(setting: Setting, groom: ActorRef) 
      extends LocalService with Curator {

  override def initializeServices {
    initializeCurator(setting.hama)
    getOrCreate("tasksCollector", classOf[TasksCollector], setting.hama)
    getOrCreate("groomCollector", classOf[GroomCollector], setting.hama)
    getOrCreate("sysMetricsCollector", classOf[SysMetricsCollector], setting.hama)
  }

  def report: Receive = {
    case r: Report =>  // TODO: write to zk
  }

  def receive = report orElse unknown

}
