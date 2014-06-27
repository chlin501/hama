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
package org.apache.hama.bsp.v2;

import java.io.IOException;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.bsp.Counters.Counter;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.io.IO;

/**
 * BSP communication interface. 
 * Exchange messages with other {@link BSPPeer}s via Writable messages.
 */
public interface BSPPeer {

  /**
   * Initialize necessary services, including 
   * - io 
   * - sync 
   * - messaging
   * @param conf contains related setting to startup related services.
  void initialize(conf: HamaConfiguration, task: Task)
   */

  /**
   * An interface that links to input and output.
   * @return IO interface that has accesses to the resource.
   */
  IO getIO();

  /**
   * Send a data with a tag to another BSPPeer corresponding to hostname.
   * Messages sent by this method are not guaranteed to be received in a sent
   * order.
   * 
   * @param peerName will be in a form of ${actor-system-name}@${host}:${port}.
   * @param msg is the type wriable that can be serialized over wire.
   * @throws IOException is thrown when io goes wrong.
   */
  void send(String peerName, Writable msg) throws IOException;

  /**
   * @return A message from the peer's received messages queue (a FIFO).
   * @throws IOException
   */
  Writable getCurrentMessage() throws IOException;

  /**
   * @return The number of messages in the peer's received messages queue.
   */
  int getNumCurrentMessages();

  /**
   * Barrier Synchronization.
   * 
   * Sends all the messages in the outgoing message queues to the corresponding
   * remote peers.
   * 
   * @throws IOException
   */
  void sync() throws IOException;

  /**
   * Superstep count at the moment this execution has.  
   * @return the count of current super-step
   */
  long getSuperstepCount();

  /**
   * The name of the peer.
   * @return the name of this peer in the format "hostname:port".
   */
  String getPeerName();

  /**
   * The <i>N</i>th peer name among all peers involved in computation.
   * @return the name of n-th peer from sorted array by name.
   */
  String getPeerName(int index);

  /**
   * The index for this bsp peer.
   * @return the index of this peer from sorted array by name.
   */
  int getPeerIndex();

  /**
   * An array of all peers' name.
   * Each peer will has a name in a form of 
   *    <pre>${$actor-system-name}@${host}:${port}</pre>
   * where actor system name is the bsp peer's system name; host the host 
   * machine name; port the machine port used by the peer.
   * @return the names of all the peers executing tasks from the same job
   *         (including this peer).
   */
  String[] getAllPeerNames();

  /**
   * The number of peers involved in computation.
   * @return the number of peers
   */
  int getNumPeers();

  /**
   * Clears all queues entries.
   */
  void clear();

  /**
   * The configuration for this job.
   * @return the job's configuration.
   */
  HamaConfiguration getConfiguration();

  /**
   * Get the {@link Counter} of the given group with the given name.
   * 
   * @param name counter name
   * @return the <code>Counter</code> of the given group/name.
  Counter getCounter(Enum<?> name);
   */

  /**
   * Get the {@link Counter} of the given group with the given name.
   * 
   * @param group counter group
   * @param name counter name
   * @return the <code>Counter</code> of the given group/name.
  Counter getCounter(String group, String name);
   */

  /**
   * Increments the counter identified by the key, which can be of any
   * {@link Enum} type, by the specified amount.
   * 
   * @param key key to identify the counter to be incremented. The key can be be
   *          any <code>Enum</code>.
   * @param amount A non-negative amount by which the counter is to be
   *          incremented.
  void incrementCounter(Enum<?> key, long amount);
   */

  /**
   * Increments the counter identified by the group and counter name by the
   * specified amount.
   * 
   * @param group name to identify the group of the counter to be incremented.
   * @param counter name to identify the counter within the group.
   * @param amount A non-negative amount by which the counter is to be
   *          incremented.
  void incrementCounter(String group, String counter, long amount);
   */

  /**
   * The task attempt id this execution holds.
   * @return the task id of this task.
   */
  TaskAttemptID getTaskAttemptId();
}
