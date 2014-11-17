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
package org.apache.hama.monitor.groom

import akka.actor.ActorRef
import org.apache.hadoop.io.Writable
import org.apache.hama.bsp.BSPJobID
import org.apache.hama.bsp.v2.Task
import org.apache.hama.monitor.Collector
import org.apache.hama.monitor.master.GroomsTracker
//import org.apache.hama.groom.GroomServerStat 

/**
 * Report GroomServer information to GroomsTracker.
 * - free/ occupied task slots
 * - slots occupied by which job relation.
 * - slot master relation. (future)
 */
final class GroomStatsCollector extends Collector {

  override def initialize() { }

  override def dest(): String = classOf[GroomsTracker].getName

  override def collect(): Writable = null.asInstanceOf[Writable]

  /**
   * Receive message from TaskCounsellor reporting GroomServerStat to 
   * {@link GroomTaskTracker}.
  def report: Receive = {
    case stat: GroomServerStat => { 
      LOG.info("Report {} stat to {}", stat.getName, reporter.path.name)
      reporter ! stat
    }
  }

  override def receive = unknown
   */
}

