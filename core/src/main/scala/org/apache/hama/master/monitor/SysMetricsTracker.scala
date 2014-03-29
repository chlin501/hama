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
package org.apache.hama.master.monitor

import org.apache.hama._
import org.apache.hama.master._
import org.apache.hama.monitor.metrics._

class SysMetricsTracker(conf: HamaConfiguration) extends LocalService {

  type GroomName = String

  var sysMetricsStat = Set.empty[MetricsRecord]

  override def configuration: HamaConfiguration = conf

  override def name: String = "sysMetricsTracker"
 
  def metricsRecord: Receive = {
    case stat: MetricsRecord => {
      LOG.info("{} sends stat {}", stat.getGroomName, stat)
      sysMetricsStat ++= Set(stat)
    }
  }

  override def receive = isServiceReady orElse metricsRecord orElse unknown
}
