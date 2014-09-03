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

/**
 * This holds task to metris stats information.
 * @param task contains specific setting for superstep computation.
 * @param counters is an object holds metrics stats data.
 */
//TODO: replace counters with e.g. asyc metrics collecting mechanism.
protected[v2] case class TaskWithStats(task: Option[Task], counters: Counters) 

object TaskOperator {

  /**
   * Constructor for task operation.
   * @param task is delivered from {@link TaskManager}.
   * @return TaskOperator holds {@link Task} and {@link Counters} objects.
   */
  def apply(task: Task): TaskOperator = 
    new TaskOperator(Some(task), new Counters)
}

class TaskOperator(task: Option[Task], counters: Counters) {

  def getThenExecute[A <: Any, B <: Any](
    v: (Task) => A
  )(e: (A) => B, default: B): B = task.map({ (aTask) => v(aTask) }).
                                       map({ (value) => e(value) }).  
                                       getOrElse(default)

  def getThenExecute[A <: Any](e: (Task) => A, default: A): A = 
    getThenExecute[Task, A]({ (t) => t})({ (value) => e(value) }, default)

  def execute(e: (Task) => Unit) = getThenExecute[Unit]({ (value) =>
    e(value) }, Unit)

}
