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
package org.apache.hama

import akka.actor.Actor
import akka.actor.ActorRef
import org.apache.hama.logging.ActorLog
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

// TODO: rename?
trait Agent extends Actor with ActorLog with Spawnable with Retryable {

  /**
   * The name for this actor. 
   * @return String is the name of this actor when created.
   */
  final def name(): String = self.path.name

  protected def unknown: Receive = {
    case msg@_ => LOG.warning("Unknown message {} for {} from {}.", 
                              msg, name, sender.path.name)
  }

  protected def shutdown() = context.system.shutdown

}
