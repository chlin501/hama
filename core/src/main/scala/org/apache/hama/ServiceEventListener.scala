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

sealed trait EventMessage
case class SubscribeEvent(event: Event, ref: ActorRef) extends EventMessage
case class UnsubscribeEvent(event: Event, ref: ActorRef) extends EventMessage

sealed trait Event
case object ServiceReady extends Event

trait ServiceEventListener { self: Actor => 
  protected var mapping = Map.empty[Event, Set[ActorRef]]

  protected def serviceEventListenerManagement: Receive = {
    case SubscribeEvent(event, ref) => {
      mapping = 
        mapping.filter( p => event.equals(p._1)).mapValues { 
          refs => refs + ref 
        }
    }
    case UnsubscribeEvent(event, ref) => {
      mapping =
        mapping.filter( p => event.equals(p._1)).mapValues { 
          refs => refs - ref 
        }
    }
  }

  protected def notify(event: Event)(message: Any): Unit = {
    mapping.filter(p => event.equals(p._1)).values.flatten.foreach( ref => {
      ref ! message
    })
  }
}

