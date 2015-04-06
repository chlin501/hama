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
package org.apache.hama.util

import java.net.InetAddress
import org.apache.hama.HamaConfiguration

object ZkUtil {

  def config(host: String = Utils.hostname, slotSeq: Int = 1, 
             port: Int = 61000, numPeers: Int = 2): 
      HamaConfiguration = {
    val conf = new HamaConfiguration
    conf.set("hama.zookeeper.quorum", "localhost:2181")
    conf.setInt("hama.zookeeper.property.clientPort", 2181)
    conf.setInt("bsp.peers.num", numPeers)
    conf.set("container.host", host)
    conf.setInt("container.port", port)
    conf.setInt("container.slot.seq", slotSeq)
    conf
  }

}
