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
import org.apache.hama.groom.TaskCounsellor
import org.apache.hama.monitor.Collector
import org.apache.hama.monitor.GetGroomStats
import org.apache.hama.monitor.GroomStats
import org.apache.hama.monitor.master.GroomsTracker

object GroomStatsCollector {

  def fullName(): String = classOf[GroomStatsCollector].getName

}

/**
 * Report GroomServer information to GroomsTracker.
 * - free/ occupied task slots
 * - slots occupation to job relation.
 */
final class GroomStatsCollector extends Collector {

  import Collector._

  private var taskCounsellor: Option[ActorRef] = None

  override def initialize() = listServices

  override def servicesFound(services: Array[ActorRef]) = services.find( s => 
    s.path.name.equalsIgnoreCase(TaskCounsellor.simpleName(configuration))
  ) match {
    case Some(found) => {
      taskCounsellor = Option(found)
      start()
    }
    case None => LOG.error("Service {} not available!", 
                           TaskCounsellor.simpleName(configuration))
  }

  // TODO: change to subscribe and task counsellor emit stats when things, e.g. slot update, change?
  override def request() = taskCounsellor.map { ref => 
    retrieve(ref, GetGroomStats) 
  }

  /**
   * WrappedCollector will wrap the stats - GroomStats - into Stats object.
   */
  override def statsCollected(stats: Writable) = report(stats)

  override def dest(): String = classOf[GroomsTracker].getName

}

