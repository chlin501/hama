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
import org.apache.hama.bsp.v2.Task
import org.apache.hama.monitor.Tracker

object JobTasksTracker {

  def fullName(): String = classOf[JobTasksTracker].getName

}

// TODO: notify to scheduler for each task update 
//       or merge to scheduler?
final class JobTasksTracker extends Tracker {

  private var tasks = Set.empty[Task]

  private var currentSuperstep = 0

  override def receive(stats: Writable) = stats match {
    case task: Task => {
      tasks += task
      val totalTasks = task.getTotalBSPTasks
      if(totalTasks == tasks.size && isNextSuperstep(totalTasks)) {
        currentSuperstep += 1
        //publish(SuperspteIncrementEvent,  TODO: add publish functions
                //SupestepIncrement(task.getId, task.getCurrentSuperstep))
      }
    }
    case other@_ => LOG.warning("Unknown task stats received: {}", other) 
  }

  private def isNextSuperstep(totalTasks: Int): Boolean = tasks.count { t => 
    t.getCurrentSuperstep == (currentSuperstep + 1) } == totalTasks

}
