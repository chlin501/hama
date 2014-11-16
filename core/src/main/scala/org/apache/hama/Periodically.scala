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

import akka.actor.ActorRef
import akka.actor.Cancellable
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration.DurationInt

sealed trait Tick
final case object Ticker extends Tick

trait Periodically { this: Agent => 

  protected def tick(target: ActorRef, message: Tick, 
                     initial: FiniteDuration = 0.seconds,
                     delay: FiniteDuration = 3.seconds): Cancellable = {
    import context.dispatcher
    context.system.scheduler.schedule(initial, delay, target, message)
  }

  protected def ticker(message: Tick)

  protected def tickMessage: Receive = {
    case t: Tick => ticker(t)
  }
 
}
