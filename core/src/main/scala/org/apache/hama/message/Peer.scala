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
package org.apache.hama.message 

import org.apache.hama.ProxyInfo

/**
 * This is used for constructing Peer information used in sending messages 
 * between {@link BSPPeer}.
 * Lookup actor doesn't requires to distinguish local from remote actor remote
 * module can correctly dispatch message to the ator as long as the peer 
 * host:port matches the target.
 */
object Peer {

  /**
   * Create remote Peer's ProxyInfo for MessageManager sending messages.
   * @param actorSystemName is the name of the actor system, found in zk client.
   * @param host is the remoe host name.
   * @param port is the port value of the remote host.
   * @return ProxyInfo contains related peer information.
   */
  def at(actorSystemName: String, host: String, port: Int): ProxyInfo = 
    ProxyInfo.fromString("akka.tcp://"+actorSystemName+"@"+host+":"+port+
                         "/user/peerMessenger")  

  /**
   * This doesn't distinguish local from remote address because remote module
   * can correctly dispatch mesage to the right actor.
   * @param peer is the actor address/ path to be used with the form of 
   *             <b>${actor-system-name}@${host}:${port}</b>.
   * @return ProxyInfo contains related peer information.
   */
  def at(peer: String): ProxyInfo = {
    if(-1 == peer.indexOf("@") || -1 == peer.indexOf(":")) 
      throw new IllegalArgumentException("Invalid peer string format: "+peer)
    ProxyInfo.fromString("akka.tcp://"+peer+"/user/peerMessenger")
  }

}
