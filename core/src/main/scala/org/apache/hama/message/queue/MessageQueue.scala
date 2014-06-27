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
package org.apache.hama.message.queue;

import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hadoop.conf.Configurable;
import org.apache.hama.HamaConfiguration;

/**
 * Message queue interface.
 */
public interface MessageQueue<M> extends Iterable<M>, Configurable {

  /**
   * Used to initialize the queue.
   * @param conf contains necessary setting to initialize queue.
   */
  void init(HamaConfiguration conf, TaskAttemptID id);

  /**
   * Finally close the queue. Commonly used to free resources.
   */
  void close();

  /**
   * Called to prepare a queue for reading.
   */
  void prepareRead();

  /**
   * Called to prepare a queue for writing.
   */
  void prepareWrite();

  /**
   * Adds a whole Java Collection to the implementing queue.
   * @param col to be added to this queue.
   */
  void addAll(Iterable<M> col);

  /**
   * Adds the other queue to this queue.
   * @param otherqueue is another message queue.
   */
  void addAll(MessageQueue<M> otherqueue);

  /**
   * Adds a single item to the implementing queue.
   * @param item to be added to this queue.
   */
  void add(M item);

  /**
   * Clears all entries in the given queue.
   */
  void clear();

  /**
   * Polls for the next item in the queue (FIFO).
   * @return a new item or null if none are present.
   */
  M poll();

  /**
   * The size of this queue, indicating how many items in this queue.
   * @return how many items are in the queue.
   */
  int size();

  /**
   * Denote if the messages are serialized to byte buffers.
   * @return boolean true if yes; otherwise false.
   */
  boolean isMessageSerialized();
  
  /**
   * Denote if this queue is memory based.
   * @return boolean true if yes; otherwise false.
   */
  boolean isMemoryBasedQueue();

}
