/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hama.sync;

import org.apache.hama.bsp.BSPJobID;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.HamaConfiguration;

object PeerDataOperator {

  val peers = "peers" 

}

/**
 * Provide functions in looking up data stored via peer.
 */
trait PeerDataOperator {


  /**
   * Registers a specific task with a its host and port to the sync daemon.
   * 
   * @param jobId the jobs ID
   * @param taskId the tasks ID
   * @param hostAddress the host where the sync server resides
   * @param port the port where the sync server is up
   */
  def register(taskAttemptId: TaskAttemptID, actorSystem: String, host: String,
               port: Int)

  /**
   * Returns all registered tasks within the sync daemon. They have to be
   * ordered ascending by their task id.
   * 
   * @param taskId the tasks ID
   * @return an <b>ordered</b> string array of host:port pairs of all tasks
   *         connected to the daemon.
   */
  def getAllPeerNames(taskAttemptId: TaskAttemptID): Array[String]

  /**
   * Peer name consisted of ${actor-system}@${host}:${port}.
   * @return String of this peer name.
   */
  def getPeerName(): String

}
