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
package org.apache.hama.bsp.v2

import org.apache.hama.bsp.Counters
import org.apache.hama.HamaConfiguration

protected[v2] case class TaskWithStats(task: Option[Task], 
                                       counters: Option[Counters]) 

trait TaskAware {

  /**
   * Beause {@link Task} is shared, change task value here would affect task
   * as the same instance somewhere else e.g. SuperstepBSP, BSPPeerContainer.
   */
  protected[v2] var taskWithStats: TaskWithStats = TaskWithStats(None, None)

  def task(): Option[Task] = taskWithStats.task

  def counters(): Option[Counters] = taskWithStats.counters

}
