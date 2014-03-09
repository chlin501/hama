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

import akka.actor._

sealed trait StateMessage
case class Subscribe(state: State, ref: ActorRef) extends StateMessage
case class Unsubscribe(state: State, ref: ActorRef) extends StateMessage

trait GroomStateListener { self: Actor => 
  protected var mapping = Map.empty[State, Set[ActorRef]]

  protected def groomStateListenerManagement: Receive = {
    case Subscribe(state, ref) => {
      mapping = 
        mapping.filter( p => state.equals(p._1)).mapValues { refs => refs+ref }
    }
    case Unsubscribe(state, ref) => {
      mapping =
        mapping.filter( p => state.equals(p._1)).mapValues { refs => refs-ref }
    }
  }

  /**
   * Notify listeners when a specific state is triggered.
   */
  protected def notifyAllWith(state: State)(message: Any): Unit = {
    mapping.filter(p => state.equals(p._1)).values.flatten.foreach( ref => {
      ref ! message
    })
  }
}

