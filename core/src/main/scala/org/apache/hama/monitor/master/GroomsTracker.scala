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
package org.apache.hama.monitor.master

import org.apache.hadoop.io.Writable
import org.apache.hama.HamaConfiguration
import org.apache.hama.monitor.Tracker
import org.apache.hama.monitor.ProbeMessages
import org.apache.hama.monitor.GroomStats

final case object GetMaxTasks extends ProbeMessages
final case class TotalMaxTasks(allowed: Int) extends ProbeMessages

final class GroomsTracker extends Tracker {

  private var allStats = Set.empty[GroomStats]

  /* total tasks allowed */
  private var totalMaxTasks: Int = 0

  override def receive(stats: Writable) = stats match {
    case stats: GroomStats => {
      allStats ++= Set(stats)
      totalMaxTasks += stats.maxTasks
    }
    case other@_ => LOG.warning("Unknown stats data: {}", other)
  }

  override def groomLeaves(name: String, host: String, port: Int) = 
    allStats.find( stats => 
      stats.name.equals(name) && stats.host.equals(host) && 
      stats.port.equals(port)
    ). map { stats => 
      allStats -= stats 
      totalMaxTasks -= stats.maxTasks
    }

  override def askFor(action: ProbeMessages): ProbeMessages = action match {
    case GetMaxTasks => TotalMaxTasks(totalMaxTasks)
  }

}
