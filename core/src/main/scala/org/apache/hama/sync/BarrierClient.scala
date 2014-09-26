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

import org.apache.hama.bsp.BSPJobID
import org.apache.hama.bsp.TaskAttemptID
import org.apache.hama.bsp.v2.Task
import org.apache.hama.LocalService
import org.apache.hama.HamaConfiguration

sealed trait BarrierMessages
final case class Enter(jobId: BSPJobID, taskAttemptId: TaskAttemptID, 
                       superstep: Long) extends BarrierMessages
final case class Leave(jobId: BSPJobID, taskAttemptId: TaskAttemptID, 
                       superstep: Long) extends BarrierMessages

object BarrierClient {

  /**
   * Create Java based barrier client object.
   * @param conf is common configuration.
   * @param task denotes which task will be used. 
   * @param host denotes the machine on which the process runs.
   * @param port denotes the port used by the process.
   */
  def client(conf: HamaConfiguration, task: Task, host: String, 
             port: Int): PeerSyncClient = {
    val client = SyncServiceFactory.getPeerSyncClient(conf)
    client.init(conf, task.getId.getJobID, task.getId)
    client.register(task.getId.getJobID, task.getId, host, port)
    client
  }

}

class BarrierClient(conf: HamaConfiguration, // common conf
                    syncClient: PeerSyncClient) extends LocalService { 

  override def configuration(): HamaConfiguration = conf

  protected def enter: Receive = {
    case Enter(jobId, taskAttemptId, superstep) => 
      syncClient.enterBarrier(jobId, taskAttemptId, superstep)
  }

  protected def leave: Receive = {
    case Leave(jobId, taskAttemptId, superstep) =>
      syncClient.leaveBarrier(jobId, taskAttemptId, superstep)
  }

  override def receive = enter orElse leave orElse unknown

}
