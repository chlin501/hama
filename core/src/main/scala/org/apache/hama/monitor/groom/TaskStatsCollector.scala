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

import org.apache.hadoop.io.Writable
import org.apache.hama.bsp.v2.Task
import org.apache.hama.groom.TaskReportEvent
import org.apache.hama.monitor.Collector
import org.apache.hama.monitor.Stats
import org.apache.hama.monitor.master.JobTasksTracker

object TaskStatsCollector {

  def fullName(): String = classOf[TaskStatsCollector].getName

}

/**
 * Collect tasks stats and send to JobTasksTracker
 */
final class TaskStatsCollector extends Collector {

  import Collector._

  private var tasks = Set.empty[Task] 

  override def initialize() = subscribe(TaskReportEvent)

  override def notified(w: Any) = w match {
    case task: Task => {
      tasks += task
      report(Stats(dest, task))
    }
    case _ => LOG.warning("Unknokwn stats {} collected!", w)
  }

  override def dest(): String = JobTasksTracker.fullName
}
