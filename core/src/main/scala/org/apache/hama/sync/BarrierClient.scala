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
import org.apache.hama.Close

sealed trait BarrierMessage
final case object GetPeerName extends BarrierMessage
final case class GetAllPeerNames(taskAttemptId: TaskAttemptID)
final case class Enter(taskAttemptId: TaskAttemptID, superstep: Long) 
      extends BarrierMessage
final case object WithinBarrier extends BarrierMessage
final case class Leave(taskAttemptId: TaskAttemptID, superstep: Long) 
      extends BarrierMessage
final case object OutsideBarrier extends BarrierMessage

object BarrierClient {

  /**
   * Create Java based barrier client object.
   * @param conf is common configuration.
   * @param taskAttemptId denotes on which task this client operate. 
   * @param host denotes the machine on which the process runs.
   * @param port denotes the port used by the process.
   */
  def client(conf: HamaConfiguration, taskAttemptId: TaskAttemptID, 
             host: String, port: Int): PeerSyncClient = {
    val client = SyncServiceFactory.getPeerSyncClient(conf)
    client.init(conf, taskAttemptId.getJobID, taskAttemptId)
    client.register(taskAttemptId.getJobID, taskAttemptId, host, port)
    client
  }

}

class BarrierClient(conf: HamaConfiguration, // common conf
                    syncClient: PeerSyncClient) extends LocalService { 

  override def configuration(): HamaConfiguration = conf

  protected def currentPeerName: Receive = {
    case GetPeerName => syncClient.getPeerName
  }

  protected def allPeerNames: Receive = {
    case GetAllPeerNames(taskAttemptId) => 
      syncClient.getAllPeerNames(taskAttemptId)
  }

  protected def enter: Receive = {
    case Enter(taskAttemptId, superstep) => {
      syncClient.enterBarrier(taskAttemptId.getJobID, taskAttemptId, superstep)
      sender ! WithinBarrier
    }
  }

  protected def leave: Receive = {
    case Leave(taskAttemptId, superstep) => {
      syncClient.leaveBarrier(taskAttemptId.getJobID, taskAttemptId, superstep)
      sender ! OutsideBarrier
    }
  }

  protected def close: Receive = {
    case Close => {
      // perform some close operations and then stop the actor
      context.stop(self) 
    }
  }

  override def receive = currentPeerName orElse allPeerNames orElse enter orElse leave orElse close orElse unknown

}
