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
import org.apache.hama.util.Curator
import org.apache.hama.conf.Setting
import org.apache.hama.logging.CommonLog

object CuratorRegistrator {

  def apply(setting: Setting): CuratorRegistrator = 
    new CuratorRegistrator(setting)
}

class CuratorRegistrator(setting: Setting) extends PeerRegistrator with Curator 
  with CommonLog {

  import PeerRegistrator._

  protected var peer: Option[SystemInfo] = None

  /**
   * Parent znode, started with '/', of peer address to be created
   * @param jobId is the job id string.
   * @return String is the parent znode of this peer system.
   */
  protected def pathTo(jobId: String): String = "/%s/%s".format(peers, jobId)

  /**
   * Peer address in the form of ${actor_system}@${host}:${port}.
   * @param actorSystem is the system that runs actors for this peer.
   * @param host is the machine for this peer.
   * @param port is that used by this peer system.
   * @return String is the peer address.
   */
  protected def toPeer(actorSystem: String, host: String, port: Int):
    SystemInfo = new SystemInfo(actorSystem, host, port)

  /**
   * Peer path is in a form of "/peers/${job_id}/${peer_address}" where 
   * peer address is consisted of ${actor_system}@${host}:${port}
   */
  protected def peerPath(pathTo: String, peerAddress: String): String = 
    "%s/%s".format(pathTo, peerAddress)

  override def register(taskAttemptId: TaskAttemptID, actorSystem: String, 
                        host: String, port: Int) {
    initializeCurator(setting)
    val p = toPeer(actorSystem, host, port)
    peer = Option(p)
    val znodePath = peerPath(pathTo(taskAttemptId.getJobID.toString), 
                             p.getAddress)
    LOG.debug("Znode path to be created {} for task {}", znodePath, 
             taskAttemptId)
    create(znodePath)
  }


  override def getAllPeerNames(taskAttemptId: TaskAttemptID): Array[String] = 
    list(pathTo(taskAttemptId.getJobID.toString))

  override def getPeerName(): String = peer match {
    case Some(p) => p.getAddress
    case None => throw new RuntimeException("Peer is not yet registered!")
  }

}
