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

import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrierV1
import org.apache.hama.bsp.TaskAttemptID
import org.apache.hama.conf.Setting
import org.apache.hama.logging.CommonLog
import org.apache.hama.util.Curator

object CuratorBarrier {

  /**
   * Create barrier instance implemented by curator.
   * @param setting is container setting.
   * @param taskAttemptId 
   * @param numBSPTasks are tasks that will meet at the barrier path.
   */  
  def apply(setting: Setting, taskAttemptId: TaskAttemptID, numBSPTasks: Int): 
    CuratorBarrier = Curator.build(setting) match {
      case null => 
        throw new RuntimeException("Can't initialize CuratorFramework!")
      case client@_ => {
        Curator.start(client)
        val root = setting.hama.get("bsp.zookeeper.root.path", "/sync")
        new CuratorBarrier(new DistributedDoubleBarrierV1(client, root, 
                           taskAttemptId, numBSPTasks))
      }
    }

}

class CuratorBarrier(barrier: DistributedDoubleBarrierV1) extends Barrier 
                                                        with CommonLog {

  override def enter(superstep: Long) = barrier.enter(superstep)

  override def leave(superstep: Long) = barrier.leave(superstep) 

}
