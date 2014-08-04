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

import akka.actor.ActorRef
import org.apache.hama.HamaConfiguration
import org.apache.hama.logging.Logger
import scala.collection.immutable.Queue

trait CheckpointerReceiver extends Logger {

  protected var ckptQueue = Queue.empty[ActorRef]

  def put(ckpt: ActorRef) = ckptQueue = ckptQueue.enqueue(ckpt)

  protected def getCommonConf(): HamaConfiguration 

  protected def currentTaskAttemptId(): String

  protected def currentSuperstepCount(): Long

  protected def isCheckpointEnabled(): Boolean =
    getCommonConf.getBoolean("bsp.checkpoint.enabled", true)

  protected def findCheckpointer(): Option[ActorRef] =
    ckptQueue.length match {
      case 0 => {
        LOG.warn("Checkpointer for "+currentTaskAttemptId+" at "+
                 "superstep "+currentSuperstepCount+" not found!")
        None
      }
      case _ => {
        val (first, rest) = ckptQueue.dequeue
        ckptQueue = rest
        Some(first)
      }
    }

  protected def getCkpt(): Option[ActorRef] = isCheckpointEnabled match {
    case true => findCheckpointer
    case false => None
  }
}
