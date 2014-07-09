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

import java.util.Iterator
import java.util.Map.Entry

import org.apache.hadoop.io.Writable
import org.apache.hadoop.util.ReflectionUtils
import org.apache.hama.HamaConfiguration
import org.apache.hama.message.compress.BSPMessageCompressor
import org.apache.hama.ProxyInfo

object OutgoingMessageManager {

  def get[M <: Writable](conf: HamaConfiguration): OutgoingMessageManager[M] = {
    val clazz = conf.getClass("hama.messenger.outgoing.message.manager.class",
                              classOf[OutgoingPOJOMessageBundle[M]],
                              classOf[OutgoingMessageManager[M]])
    val out = ReflectionUtils.newInstance(clazz, conf)
    out.init(conf, BSPMessageCompressor.get(conf))
    out
  }

}

trait OutgoingMessageManager[M <: Writable] {

  /**
   * Initialize outgoing message manager.
   * @param conf is common configuration, not specific for task.
   * @param compressor tells how messages to be compressed.
   */
  def init(conf: HamaConfiguration, compressor: BSPMessageCompressor)

  /**
   * Add a message, classified by the peer info, to outgoing queue.
   * @param peerInfo is consisted of ${actor-system-name}@${host}:${port}
   * @param msg is a writable message to be sent.
   */
  def addMessage(peerInfo: ProxyInfo, msg: M)

  /**
   * Clear the outgoing queue.
   */
  def clear()

  /**
   * Iterator of the entire messages.
   * @return Iterator contains peer info associated with message bundles.
   */
  def getBundleIterator(): java.util.Iterator[java.util.Map.Entry[ProxyInfo, BSPMessageBundle[M]]] 

}
