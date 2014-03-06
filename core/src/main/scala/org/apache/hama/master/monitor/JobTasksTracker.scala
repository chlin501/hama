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
import org.apache.hama.bsp.BSPJobID
import org.apache.hama.bsp.v2.Task
import org.apache.hama.master._

final class JobTasksTracker(conf: HamaConfiguration) extends Service(conf) {

  var tasksMapping = Map.empty[BSPJobID, Task]

  override def name: String = "jobTasksTracker"

  override def receive = {
    ready orElse
    ({case Report(newTask) => { 
      val bspJobId = newTask.getId.getJobID
      tasksMapping.get(bspJobId) match {
        case Some(oldTask) => tasksMapping ++= Map(bspJobId -> newTask)
        case None =>  tasksMapping ++= Map(bspJobId -> newTask)
      }
    }}: Receive) orElse unknown
  } 


}
