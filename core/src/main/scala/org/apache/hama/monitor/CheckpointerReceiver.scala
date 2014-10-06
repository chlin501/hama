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
/*
import org.apache.hama.HamaConfiguration
import org.apache.hama.logging.CommonLog
import scala.collection.immutable.Queue

 * This is used to identify if the target needs {@link Checkpointer}. It is
 * generally applied in {@link BSPPeer#sync} implementation.
trait CheckpointerReceiver extends CommonLog {

   * The queue that stores checkpoint actor.
  protected[monitor] var packQueue = Queue.empty[Pack]

   * Receive checkpointer, and store it in a queue.
   * This function is called by {@link SuperstepBSP}.
   * @param pack is the checkpointer actor.
  def receive(pack: Pack) = packQueue = packQueue.enqueue(pack)
  
   * Check if the checkpoint is enabled in {@link HamaConfiguration}; default 
   * set to false.
   * @param conf is common configuration.
   * @return Boolean denote true if checkpoint is enabled; othwerwise false.
  protected def isCheckpointEnabled(conf: HamaConfiguration): Boolean =
    conf.getBoolean("bsp.checkpoint.enabled", true)

  protected def queueLength(): Int = packQueue.length

   * Retrieve the first checkpointer found in queue. 
   * This doesn't verify if 
   * <pre>
   * true == HamaConfiguration.get("bsp.checkpoint.enabled")
   * </pre>
   * @return Option[Pack] of checkpoint actor.
  protected[monitor] def head(): Option[Pack] = queueLength match {
    case 0 => None
    case _ => {
      val (first, rest) = packQueue.dequeue
      packQueue = rest
      Option(first)
    }
  }

   * Verify if checkpoint is enabled in {@link HamaConfiguration}. If true, 
   * return the first checkpoint in queue; otherwise {@link scala.None} is 
   * returned.
  def nextPack(conf: HamaConfiguration): Option[Pack] = 
      isCheckpointEnabled(conf) match {
    case true => head
    case false => None  
  } 

}
*/
