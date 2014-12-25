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
private[groom] final case class ContainerReady(seq: Int) extends GroomMessage
 */

private[groom] final case class ProcessReady(seq: Int) extends GroomMessage

private[groom] final case class PullForExecution(
  slotSeq: Int
) extends GroomMessage

private[groom] final case class RemoveTask(
  slotSeq: Int, taskAttemptId: TaskAttemptID
) extends GroomMessage

/**
 * Client notifies {@link TaskCounsellor} to stop {@link Executor}.
 */
private[groom] final case class StopExecutor(slotSeq: Int) extends GroomMessage
