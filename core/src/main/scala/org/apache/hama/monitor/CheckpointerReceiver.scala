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
package org.apache.hama.monitor

import org.apache.hama.HamaConfiguration
import org.apache.hama.logging.Logger
import scala.collection.immutable.Queue

/**
 * This is used to identify if the target needs {@link Checkpointer}. It is
 * generally applied in {@link BSPPeer#sync} implementation.
 */
trait CheckpointerReceiver extends Logger {

  /**
   * The queue that stores checkpoint actor.
   */
  protected[monitor] var packQueue = Queue.empty[Pack]

  /**
   * Receive checkpointer, and store it in a queue.
   * This function is called by {@link SuperstepBSP}.
   * @param pack is the checkpointer actor.
   */
  def receive(pack: Pack) = packQueue = packQueue.enqueue(pack)

  /**
   * Used to retrieve setting stored in common confiuration.
   */
  protected[monitor] def getCommonConf(): HamaConfiguration 

  /**
   * Obtain current task atempt id for each task will only be specific to a 
   * task id. 
   * @return String of the task attempt id.
   */
  protected[monitor] def currentTaskAttemptId(): String

  /**
   * Identify the current superstep count value.
   * @return Long of the superstep count.
   */ 
  protected[monitor] def currentSuperstepCount(): Long

  /**
   * Check if the checkpoint is enabled in {@link HamaConfiguration}; default 
   * set to true.
   * @return Boolean denote true if checkpoint is enabled; othwerwise false.
   */
  protected[monitor] def isCheckpointEnabled(): Boolean =
    getCommonConf.getBoolean("bsp.checkpoint.enabled", true)

  protected def queueLength(): Int = packQueue.length

  /**
   * Retrieve the first checkpointer found in queue. 
   * This doesn't verify if 
   * <pre>
   * true == HamaConfiguration.get("bsp.checkpoint.enabled")
   * </pre>
   * @return Option[Pack] of checkpoint actor.
   */
  protected[monitor] def firstPackInQueue(): Option[Pack] = queueLength match {
    case 0 => {
      LOG.warn("Checkpointer for "+currentTaskAttemptId+" at "+
               "superstep "+currentSuperstepCount+" not found!")
      None
    }
    case _ => {
      val (first, rest) = packQueue.dequeue
      packQueue = rest
      Some(first)
    }
  }

  /**
   * Verify if checkpoint is enabled in {@link HamaConfiguration}. If true, 
   * return the first checkpoint in queue; otherwise {@link scala.None} is 
   * returned.
   */
  def nextPack(): Option[Pack] = isCheckpointEnabled match {
    case true => firstPackInQueue
    case false => None
  } 
}
