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
package org.apache.hama.sync

import org.apache.hama.bsp.TaskAttemptID
import org.apache.hama.SystemInfo
import org.apache.hama.HamaConfiguration
import org.apache.hama.util.Curator

class CuratorPeerDataOperator(conf: HamaConfiguration) extends PeerDataOperator
                                                       with Curator {

  import PeerDataOperator._

  /**
   * Parent znode of peer address to be created.
   * @param jobId is the job id string.
   * @return String is the parent znode of this peer system.
   */
  protected def pathTo(jobId: String): String = "%s/%s".format(peers, jobId)

  /**
   * Peer address in the form of ${actor_system}@${host}:${port}.
   * @param actorSystem is the system that runs actors for this peer.
   * @param host is the machine for this peer.
   * @param port is that used by this peer system.
   * @return String is the peer address.
   */
  protected def peerAddress(actorSystem: String, host: String, port: Int):
    String = new SystemInfo(actorSystem, host, port).getAddress

  /**
   * Peer path is in the form of peers/${job_id}/${peer_address} where 
   * peer address is consisted of ${actor_system}@${host}:${port}
   */
  protected def peerPath(pathTo: String, peerAddress: String): 
     String = {
    "%s/%s".format(pathTo, peerAddress)
  }

  override def register(taskAttemptId: TaskAttemptID, actorSystem: String, 
                        host: String, port: Int) {
    initializeCurator(conf)
    val znodePath = peerPath(pathTo(taskAttemptId.getJobID.toString), 
                             peerAddress(actorSystem, host, port))
    create(znodePath)
  }


  override def getAllPeerNames(taskAttemptId: TaskAttemptID): Array[String] = 
    list(pathTo(taskAttemptId.getJobID.toString))

}
