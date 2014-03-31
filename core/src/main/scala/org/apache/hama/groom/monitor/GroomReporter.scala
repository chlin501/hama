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
package org.apache.hama.groom.monitor

import akka.actor._
import org.apache.hama._
import org.apache.hama.groom._
import org.apache.hama.bsp.BSPJobID
import org.apache.hama.bsp.v2.Task
import org.apache.hama.master._

/**
 * Report GroomServer information.
 * - free/ occupied task slots
 * - slots occupied by which job relation.
 * - slot master relation. (future)
 */
final class GroomReporter(conf: HamaConfiguration) extends LocalService 
                                                   with RemoteService {
  var tracker: ActorRef = _

  val groomTasksTrackerInfo =
    ProxyInfo("groomTasksTracker",
              conf.get("bsp.master.actor-system.name", "MasterSystem"),
              conf.get("bsp.master.address", "127.0.0.1"),
              conf.getInt("bsp.master.port", 40000),
              "bspmaster/monitor/groomTasksTracker")

  val groomTasksTrackerPath = groomTasksTrackerInfo.path

  override def configuration: HamaConfiguration = conf

  override def name: String = "groomReporter"

  override def initializeServices {
    lookup("groomTasksTracker", groomTasksTrackerPath)
  }

  override def afterLinked(proxy: ActorRef) = tracker = proxy

  def report: Receive = {
    case stat: GroomStat => { 
      LOG.info("Report groom server {} to {}", stat.groomName, 
               tracker.path.name)
      tracker ! stat
    }
  }

  override def receive = isServiceReady orElse report orElse isProxyReady orElse timeout orElse unknown
}
