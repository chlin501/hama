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
package org.apache.hama.master

import org.apache.hama.bsp.BSPJobID
import org.apache.hama.bsp.v2.Job

sealed trait MasterMessage

private[master] final case class Dispense(ticket: Ticket) extends MasterMessage

/**
private[master] final case object GetJobSeq extends MasterMessage

private[master] final case object GetMasterId extends MasterMessage

 * This is used to notify Scheduler that a GroomServer's registered.
 * Scheduler can simply use {@link #taskCounsellor} to dispatch tasks.
 * @param groomServerName is the GroomServer name .
 * @param taskCounsellor refers to GroomServer's TaskCounsellor.
 * @param maxTasks denote the capacity the GroomServer has upon registration.
private[master] final case class GroomEnrollment(
  groomServerName: String, taskCounsellor: ActorRef, maxTasks: Int
) extends MasterMessage

private[master] final case class JobSeq(id: Int) extends MasterMessage
 */

private[master] final case object JobSubmission extends MasterMessage

private[master] final case class MasterId(
  id: Option[String]
) extends MasterMessage

private[master] final case class RescheduleTasks(
  groomServerName: String
) extends MasterMessage

final case class Submit(jobId: BSPJobID, jobFilePath: String) 
      extends MasterMessage 

/**
 * A message denotes to take a job from Receptionist's waitQueue.
 */
private[master] final case object TakeFromWaitQueue extends MasterMessage
