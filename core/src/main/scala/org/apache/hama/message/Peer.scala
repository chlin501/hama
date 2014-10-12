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
 * This is used for constructing Peer information when sending messages between
 * {@link BSPPeer}.
 * Actor lookup doesn't require distinguishing local from remote because remote
 * module can correctly dispatch message to the actor as long as the peer 
 * host:port vlaue matches the target.
 */
object Peer {

  /**
   * Create remote Peer's Proxy infomation for MessageManager.
   * @param actorSystemName is the name of the actor system, found in zk client.
   * @param host is the remoe host name.
   * @param port is the port value of the remote host.
   * @return ProxyInfo contains related peer information.
   */
  def at(actorSystem: String, host: String, port: Int): ProxyInfo = {
    val identifier = actorSystem+"@"+host+":"+port
    ProxyInfo.fromString("akka.tcp://"+identifier+"/user/container/messenger-"+
                         identifier.replaceAll("@", "_").replaceAll(":", "_"))
  }

  

  /**
   * Create Peer's Proxy information for MessageManager.
   * @param peer is the actor address/ path to be used with the form of 
   *             <b>${actor-system-name}@${host}:${port}</b>.
   * @return ProxyInfo contains related peer information.
   */
  def at(peer: String): ProxyInfo = 
    if(-1 == peer.indexOf("@") || -1 == peer.indexOf(":")) {
      ProxyInfo.fromString("akka://"+peer+"/user/container/messenger-"+peer)
      
    } else {
      ProxyInfo.fromString("akka.tcp://"+peer+"/user/container/messenger-"+
                           peer.replaceAll("@", "_").replaceAll(":", "_"))
    }
}
