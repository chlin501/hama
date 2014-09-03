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
import org.apache.hama.util.Utils._

protected[v2] case class TaskWithStats(task: Task, counters: Counters) 

object TaskOperator {

  def apply(task: Task): TaskOperator = task match {
    case null => throw new IllegalArgumentException("Task is missing!")
    case _=> new TaskOperator(TaskWithStats(task, new Counters))
  }
}

class TaskOperator(taskWithStats: TaskWithStats) {

  /**
   * Beause {@link Task} is shared, change task value here would affect task
   * as the same instance somewhere else e.g. SuperstepBSP, BSPPeerContainer.
  protected[v2] var taskWithStats = TaskWithStats(None, new Counters)
   */

  //def bind(aTask: Task) = taskWithStats = TaskWithStats(aTask, counters)

  def task(): Task = taskWithStats.task

  def counters(): Counters = taskWithStats.counters

/*
  def whenFound[A <: Any](f: (Task) => A, default: A): A = 
    doIfExists[Task, A](task, { (found) => f(found) }, default)

  def whenFound(f: (Task) => Unit) = 
    doIfExists[Task, Unit](task, { (found) => f(found) }, Unit)
*/
}
