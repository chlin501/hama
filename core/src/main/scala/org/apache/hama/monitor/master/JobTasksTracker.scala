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

import org.apache.hama.HamaConfiguration
import org.apache.hama.bsp.BSPJobID
import org.apache.hama.bsp.v2.Task
import org.apache.hama.monitor.Tracker

final class JobTasksTracker extends Tracker {

  override def initialize() { }

/*
  private var tasksStat = Map.empty[BSPJobID, Set[Task]]

  private def reportTask: Receive = {
    case newTask: Task => {  // report
      val bspJobId = newTask.getId.getJobID
      LOG.info("Report task with BSPJobID {}.", bspJobId)
      tasksStat.find(p=> p._1.equals(bspJobId)) match {
        case Some((id, oldTasks)) => 
          tasksStat ++= Map(bspJobId -> (Set(newTask)++oldTasks))
        case None => tasksStat ++= Map(bspJobId -> Set(newTask))
      }
    }
  }

  override def receive = reportTask orElse unknown
*/

}
