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
package org.apache.hama.message.queue

import org.apache.hadoop.conf.Configurable
import org.apache.hadoop.util.ReflectionUtils
import org.apache.hama.bsp.TaskAttemptID
import org.apache.hama.HamaConfiguration

object MessageQueue {

  /**
   * Obtain message queue without initialized to a specific task attempt id.
   * Note that binding MessageQueue to a specific task one needs to call 
   * <pre>MessageQueue.init(HamaConfiguration, TaskAttemptID)</pre> method
   * for related setting to be initialized.
   * @param conf is commons setting not specific to a task configuration.
   * @return MessageQueue[M] without calling init() method.
   */
  def get[M](conf: HamaConfiguration): MessageQueue[M] = {
    val clazz = conf.getClass("hama.messenger.receive.queue.class",
                              classOf[MemoryQueue[M]], 
                              classOf[MessageQueue[M]])
    ReflectionUtils.newInstance(clazz, conf)
  }

}

/**
 * Message queue interface.
 */
trait MessageQueue[M] extends java.lang.Iterable[M] with Configurable {

  /**
   * Initialize the queue and bind to a specific task id.
   * @param conf contains necessary setting to initialize queue.
   */
  def init(conf: HamaConfiguration, id: TaskAttemptID)

  /**
   * Finally close the queue. Commonly used to free resources.
   */
  def close()

  /**
   * Called to prepare a queue for reading.
   */
  def prepareRead()

  /**
   * Called to prepare a queue for writing.
   */
  def prepareWrite()

  /**
   * Adds a whole Java Collection to the implementing queue.
   * @param col to be added to this queue.
   */
  def addAll(collection: java.lang.Iterable[M])

  /**
   * Adds the other queue to this queue.
   * @param otherqueue is another message queue.
   */
  def addAll(otherqueue: MessageQueue[M]);

  /**
   * Adds a single item to the implementing queue.
   * @param item to be added to this queue.
   */
  def add(item: M)

  /**
   * Clears all entries in the given queue.
   */
  def clear()

  /**
   * Polls for the next item in the queue (FIFO).
   * @return a new item or null if none are present.
   */
  def poll(): M

  /**
   * The size of this queue, indicating how many items in this queue.
   * @return how many items are in the queue.
   */
  def size(): Int

  /**
   * Denote if the messages are serialized to byte buffers.
   * @return boolean true if yes; otherwise false.
   */
  def isMessageSerialized(): Boolean
  
  /**
   * Denote if this queue is memory based.
   * @return boolean true if yes; otherwise false.
   */
  def isMemoryBasedQueue(): Boolean

}
