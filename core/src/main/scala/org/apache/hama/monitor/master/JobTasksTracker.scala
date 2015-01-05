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
import org.apache.hama.bsp.BSPJobID
import org.apache.hama.bsp.v2.Task
import org.apache.hama.monitor.PublishEvent
import org.apache.hama.monitor.Tracker

final case object SuperstepIncrementEvent extends PublishEvent
final case class LatestSuperstep(jobId: BSPJobID, superstep: Int, 
                                 totalTasks: Int)

object JobTasksTracker {

  def fullName(): String = classOf[JobTasksTracker].getName

}

final class JobTasksTracker extends Tracker {

  private var tasks = Set.empty[Task]

  private var currentSuperstep = 0 

  // TODO: subscribe to sched's Job Finished event and clean up tasks set.
  override def receive(stats: Writable) = stats match {
    case task: Task => {
      if(tasks.isEmpty) currentSuperstep = task.getCurrentSuperstep
      tasks += task
      val totalTasks = task.getTotalBSPTasks
      // TODO: publish task arrival event and sched subscribe for notification
      //       publish(TaskArrival, task) 
      if(totalTasks == tasks.size && goToNextSuperstep(totalTasks)) {
        currentSuperstep += 1
        publish(SuperstepIncrementEvent,  
                LatestSuperstep(task.getId.getJobID, task.getCurrentSuperstep,
                                totalTasks))
      }
    }
    case other@_ => LOG.warning("Unknown task stats received: {}", other) 
  }

  private def goToNextSuperstep(totalTasks: Int): Boolean = tasks.count { t => 
    t.getCurrentSuperstep == (currentSuperstep + 1) } == totalTasks

}
