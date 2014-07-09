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

import java.util.HashMap
import java.util.Iterator
import java.util.Map.Entry

import org.apache.hadoop.io.Writable
import org.apache.hama.bsp.Combiner
import org.apache.hama.Constants
import org.apache.hama.HamaConfiguration
import org.apache.hama.message.compress.BSPMessageCompressor
import org.apache.hama.ProxyInfo
import org.apache.hama.util.ReflectionUtils

class OutgoingPOJOMessageBundle[M <: Writable] 
      extends OutgoingMessageManager[M] {

  type PeerAndBundleIter = Iterator[Entry[ProxyInfo, BSPMessageBundle[M]]] 

  private var conf: HamaConfiguration = _
  private var compressor: BSPMessageCompressor = _
  private var combiner: Combiner[M] = _
  private val peerSocketCache = new HashMap[String, ProxyInfo]()
  private val outgoingBundles = new HashMap[ProxyInfo, BSPMessageBundle[M]]()

  override def init(conf: HamaConfiguration, 
                    compressor: BSPMessageCompressor) {
    this.conf = conf
    this.compressor = compressor
    val combinerName = conf.get(Constants.COMBINER_CLASS)
    if (null != combinerName) {
      try {
        this.combiner = ReflectionUtils.newInstance(conf
            .getClassByName(combinerName)).asInstanceOf[Combiner[M]]
      } catch {
       case cnfe: ClassNotFoundException => cnfe.printStackTrace()
      }
    }
  }

  override def addMessage(peer: ProxyInfo, msg: M) {
    
    if (null != combiner) {
      val bundle = outgoingBundles.get(peer)
      bundle.addMessage(msg)
      val combined = new BSPMessageBundle[M]()
      combined.setCompressor(compressor, conf.getLong(
        "hama.messenger.compression.threshold", 128)
      )
      combined.addMessage(combiner.combine(bundle))
      outgoingBundles.put(peer, combined)
    } else {
      outgoingBundles.get(peer).addMessage(msg)
    }
  }

/* This is needed when addMessage with peerName (String) as param
  def getPeerInfo(peerName: String): ProxyInfo = {
    var peer: ProxyInfo = null
    if (peerSocketCache.containsKey(peerName)) {
      peer = peerSocketCache.get(peerName)
    } else {
      peer = Peer.at(peerName)
      peerSocketCache.put(peerName, peer) 
    }

    if (!outgoingBundles.containsKey(peer)) {
      val bundle = new BSPMessageBundle[M]()
      bundle.setCompressor(compressor,
          conf.getLong("hama.messenger.compression.threshold", 128))
      outgoingBundles.put(peer, bundle)
    }
    peer 
  }
*/

  override def clear() {
    outgoingBundles.clear
  }

  override def getBundleIterator(): java.util.Iterator[java.util.Map.Entry[ProxyInfo, BSPMessageBundle[M]]] = { outgoingBundles.entrySet.iterator }

}
