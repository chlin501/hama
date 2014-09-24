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

import akka.actor.ActorRef
import java.io.IOException
import java.util.Iterator
import java.util.Map.Entry
import org.apache.hadoop.util.ReflectionUtils
import org.apache.hadoop.io.Writable
import org.apache.hama.HamaConfiguration
import org.apache.hama.ProxyInfo
import org.apache.hama.bsp.TaskAttemptID

/**
 * Communication between {@link BSPPeer}s.
 */
trait MessageManager[M <: Writable] {

  /**
   * Initialize message manager with specific {@link TaskAttemptID} and setting.
   * @param configuration that contains necessary setting for initialize 
   *                      message manager.
   * @param id denotes which task attempt id this message manager will manage.
  def init(conf: HamaConfiguration, id: TaskAttemptID) // TODO: rename to setup
   */

  /**
   * Close underlying operations.
   * - localQueue
   * - outgoingMessageManager
   */
  def close()

  /**
   * Get the current message.
   * @throws IOException
   */
  @throws(classOf[IOException])
  def getCurrentMessage(): M 

  /**
   * Send a message to a specific {@link BSPPeer}, denoted by peerName.
   * @throws IOException
   */
  @throws(classOf[IOException])
  def send(peerName: String, msg: M) 

  /**
   * Returns an bundle of messages grouped by {@link BSPPeer}. 
   * @return an iterator that contains messages associated with a peer address.
   */
  def getOutgoingBundles(): Iterator[Entry[ProxyInfo, BSPMessageBundle[M]]] 

  /**
   * Wrap peer into an object which should contain all necessary information.
   * @param peer info is stored inside this object.
   * @param bundle are message to be sent.
   */
  @throws(classOf[IOException])
  def transfer(peer: ProxyInfo, bundle: BSPMessageBundle[M])

  @throws(classOf[IOException])
  def transfer(peer: ProxyInfo, bundle: BSPMessageBundle[M], actor: ActorRef)

  /**
   * Clears the outgoing message queue. 
   */
  def clearOutgoingMessages()

  /**
   * Gets the number of messages in the current queue.
   * @return the number of messages in the current queue.
   */
  def getNumCurrentMessages(): Int

  /**
   * Send the messages to self to receive in the next superstep.
   * @param bundle contains messages to be sent to itself.
   */
  @throws(classOf[IOException])
  def loopBackMessages(bundle: BSPMessageBundle[M]) 

  /**
   * Send the message to self. Messages will be received at the next superstep.
   * @param message is what to be sent to itself.
   */
  @throws(classOf[IOException])
  def loopBackMessage(message: Writable) 

  /**
   * Returns the server address on which the incoming connections are listening.
   * @param InetSocketAddress to which this server listens.
   */
  def getListenerAddress(): ProxyInfo
}
