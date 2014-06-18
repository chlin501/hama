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
package org.apache.hama.message;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.message.MessageManager;

/**
 * Communication between {@link BSPPeer}s.
 */
public interface MessageManager<M extends Writable> {

  /**
   * Initialize message manager with specific {@link TaskAttemptID} and setting.
   * @param configuration that contains necessary setting for initialize 
   *                      message manager.
   * @param id denotes which task attempt id this message manager will manage.
   */
  void init(HamaConfiguration configuration, TaskAttemptID id);

  /**
   * Close the communication between {@link BSPPeer}s.
   */
  void close();

  /**
   * Get the current message.
   * @throws IOException
   */
  M getCurrentMessage() throws IOException;

  /**
   * Send a message to a specific {@link BSPPeer}, denoted by peerName.
   * @throws IOException
   */
  void send(String peerName, M msg) throws IOException;

  /**
   * Returns an bundle of messages grouped by {@link BSPPeer}. 
   * @return an iterator that contains messages associated with a peer address.
   */
  Iterator<Entry<InetSocketAddress, BSPMessageBundle<M>>> getOutgoingBundles();

  /**
   * Start transferring message bundle to a specific {@link BSPPeer}.
   * @param addr denotes the target address.  
   * @param bundle are message to be tranferred. 
  void transfer(InetSocketAddress addr, BSPMessageBundle<M> bundle)
      throws IOException;
   */

  /**
   * Wrap peer into an object which should contain all necessary information.
   * @param peer info is stored inside this object.
   * @param bundle are message to be sent.
   */
  void transfer(PeerInfo peer, BSPMessageBundle<M> bundle)
      throws IOException;

  /**
   * Clears the outgoing message queue. 
   */
  void clearOutgoingMessages();

  /**
   * Gets the number of messages in the current queue.
   * @return the number of messages in the current queue.
   */
  int getNumCurrentMessages();

  /**
   * Send the messages to self to receive in the next superstep.
   * @param bundle contains messages to be sent to itself.
   */
  void loopBackMessages(BSPMessageBundle<M> bundle) throws IOException;

  /**
   * Send the message to self. Messages will be received at the next superstep.
   * @param message is what to be sent to itself.
   */
  void loopBackMessage(Writable message) throws IOException;

  /**
   * Returns the server address on which the incoming connections are listening.
   * @param InetSocketAddress to which this server listens.
   */
  InetSocketAddress listenerAddress();

}
