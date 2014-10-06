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
import org.apache.hadoop.io.Writable
import org.apache.hama.bsp.v2.Superstep
import org.apache.hama.ProxyInfo
import org.apache.hama.message.BSPMessageBundle

sealed trait CheckpointMessage
final case object StartCheckpoint extends CheckpointMessage
final case class LocalQueueMessages[M <: Writable](list: List[M]) 
      extends CheckpointMessage
final case object EmptyLocalQueue extends CheckpointMessage

/**
 * Checkpoint {@link Superstep}'s variables map, {@link Superstep} next
 * superstep class, and messages in localQueue, which is {@link MessageQueue}.
 * @param variables is a map containing custom defined values during superstep.
 *                  computation.
 * @param nextSuperstep is the next superstep to be executed.
 * @param localMessages are messages, sent from other nodes or by itself, 
 *                      stored in local queue.
final case class Checkpoint[M <: Writable](
  variables: Map[String, Writable], nextSuperstep: Class[_ <: Superstep],
  localMessages: List[M] 
) extends CheckpointMessage
 */

/**
 * Pack {@link Checkpointer}, {@link Superstep}'s variables, and next 
 * {@link Superstep} class for checkpoint process.
 * Checkpoint process will save the current superstep, e.g. the N-th superstep,
 * variables and the next (the N+1 th) superstep class. So during recovery, 
 * (assuming computation fails at the N+1 superstep,) nextSuperstep can be 
 * applied as the current superstep, and variables can be restored as 
 * nextSupestep's variables for computation.
 * @param ckpt is the Checkpointer actor. 
 * @param variable is a {@link scala.collection.immutable.Map} 
 * @param nextSuperstep is next superstep class to be executed.
final case class Pack(ckpt: Option[ActorRef], 
                      variables: Map[String, Writable],
                      nextSuperstep: Class[_ <: Superstep]
) extends CheckpointMessage
 */

