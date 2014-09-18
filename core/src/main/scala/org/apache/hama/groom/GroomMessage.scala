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
package org.apache.hama.groom

import akka.actor.ActorRef
import org.apache.hama.bsp.TaskAttemptID
import org.apache.hama.bsp.v2.Task

sealed trait GroomMessage

/**
 * Singnify Container actor is ready.
 */
private[groom] final case object ContainerReady extends GroomMessage

/**
 * Singnify Container actor is stopped.
 */
private[groom] final case object ContainerStopped extends GroomMessage

private[groom] final case class PullForExecution(
  slotSeq: Int
) extends GroomMessage

private[groom] final case class RemoveTask(
  slotSeq: Int, taskAttemptId: TaskAttemptID
) extends GroomMessage

/**
 * A slot holds relation from its id sequence to a specific
 * {@link org.apache.hama.bsp.v2.Task}.
 * @param seq of this slot.
 * @param task that runs on this slot.
 * @param master to which this slot belongs.
 * @param executor that executes the task for this slot.
 */
private[groom] final case class Slot(
  seq: Int, task: Option[Task], master: String, executor: Option[ActorRef]
) extends GroomMessage

/**
 * Client notifies {@link TaskManager} to stop {@link Executor}.
 */
private[groom] final case class StopExecutor(slotSeq: Int) extends GroomMessage
