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

import java.net.InetSocketAddress

object PeerInfo {

  def apply(actorSystemName: String, host: String, port: Int): PeerInfo = 
    new PeerInfo(actorSystemName, new InetSocket(host, port))

  def fromString(info: String): PeerInfo = {
    if(null == info || info.isEmpty) 
      throw new IllegalArgumentException("PeerInfo string do not have data.")
    val ary = info.split("@")
    if(2 != ary.length) 
      throw new RuntimeException("Invalid PeerInfo string format: "+info)
    val hostPort = ary(1).split(":")
    if(2 != hostPort.length)
      throw new RuntimeException("Invalid host:port string format: "+info)
    new PeerInfo(ary(0), hostPort(0), hostPort(1))
  }
}

/**
 * Store actor system information, including 
 * - actor system name
 * - host
 * - port 
 * for {@link BSPPeer} that runs on it.
 * @param actorSystenName denotes the name of the actor system on which the 
 *                        peer runs.
 * @param socket conains {@link InetAddress} and port information.
 */
final case class PeerInfo(actorSystemName: String, socket: InetSocketAddress) {

  def this(actorSystemName: String, host: String, port: Int) = 
    this(actorSystemName, new InetSocketAddress(host, port))

  if(null == actorSystemName || actorSystemName.isEmpty)
    throw new IllegalArgumentException("Actor system name is missing!")

  if(null == socket) 
    throw new IllegalArgumentException("Ip and port is not provided!")

  def host: String = socket.getAddress.getHostAddress

  def port: Int = socket.getPort

}
