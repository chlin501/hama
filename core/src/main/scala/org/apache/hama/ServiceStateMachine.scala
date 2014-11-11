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
import akka.actor.FSM
import akka.actor.Cancellable
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration.DurationInt

sealed trait HamaServices
private case class Cache(services: Set[ActorRef]) extends HamaServices

/**
 * This trait defines generic states to be used.
 */
trait ServiceStateMachine extends FSM[ServiceState, HamaServices] {

  startWith(StartUp, Cache(Set.empty[ActorRef]))

  /**
   * Handle events in StartUp state.
   */
  when(StartUp) {
    case Event(Next, s @ Cache(subServices)) => {
      goto(Normal) using s
    }
  }

  /**
   * Handle events in Normal state.
   */
  when(Normal) {
    case Event(Next, s @ Cache(subServices)) => {
      goto(CleanUp) using s
    }
  }

  /**
   * Handle events in CleanUp state.
   */
  when(CleanUp) {
    case Event(Next, s @ Cache(subServices)) => {
      goto(Stopped) using s
    }
  }

  initialize()
}
