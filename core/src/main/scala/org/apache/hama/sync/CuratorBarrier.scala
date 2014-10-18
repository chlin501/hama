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
package org.apache.hama.sync

import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier
import org.apache.hama.HamaConfiguration
import org.apache.hama.bsp.TaskAttemptID
import org.apache.hama.util.Curator

object CuratorBarrier {

  def barrierPath(superstep: Long, taskAttemptId: TaskAttemptID): String = 
    "%s/%s".format(taskAttemptId.getJobID.toString, superstep)

  /**
   * Create barrier instance implemented by curator.
   * @param conf common conf contains curator information.
   * @param barrierPath points to redevouz path used by curator receipe.
   * @param numBSPTasks are tasks that will meet at the barrier path.
   */
  def apply(conf: HamaConfiguration, barrierPath: String, numBSPTasks: Int): 
      CuratorBarrier = Curator.build(conf) match {
    case null => 
      throw new RuntimeException("Can't initialize CuratorFramework!")
    case client@_ => 
      new CuratorBarrier(new DistributedDoubleBarrier(client, barrierPath, 
                                                      numBSPTasks))
  }

}

class CuratorBarrier(barrier: DistributedDoubleBarrier) extends Barrier {

  override def enter() = barrier.enter

  override def leave() = barrier.leave

}
