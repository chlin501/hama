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

/**
 * A marker interface for event object.
 */
trait Event 

sealed trait EventMessage
/**
 * Subscribe to listening to specific events.
 */
case class SubscribeEvent(events: Event*) extends EventMessage
/**
 * Unsubscribe to specific events.
 */
case class UnsubscribeEvent(events: Event*) extends EventMessage

trait EventListener { self: Agent => 

  protected var mapping = Map.empty[Event, Set[ActorRef]]

  protected def eventListenerManagement: Receive = {
    case s: SubscribeEvent => s.events.foreach ( event => 
      mapping = mapping.filter( p => event.equals(p._1)).
                        mapValues { refs => refs + sender }
    )
    case us: UnsubscribeEvent => us.events.foreach( event => 
      mapping = mapping.filter( p => event.equals(p._1)).
                        mapValues { refs => refs - sender }
    )
  }

  /**
   * Notify listener with a specific message.
   */
  protected def notify(event: Event)(message: Any): Unit = mapping.filter( p =>
    event.equals(p._1)).values.flatten.foreach( ref => ref ! message )

  /**
   * Forward message to a specific listener.
   */
  protected def forward(event: Event)(message: Any): Unit = mapping.filter( p =>
    event.equals(p._1)).values.flatten.foreach( ref => ref forward message )

}

