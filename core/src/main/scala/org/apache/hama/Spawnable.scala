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
import akka.actor.Props
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration.DurationInt

trait Spawnable { this: Agent => 

  /**
   * Spawn a child.
   * @param childName is the name of child actor.
   * @param actorClass is the actor class to be spawned.
   * @param args is variable args to be used by the actor.
   * @return ActorRef is the created actor instance.
   */
  protected def spawn[A <: Actor](childName: String, actorClass: Class[A],
                                  args: Any*): ActorRef =  
    context.actorOf(Props(actorClass, args:_*), childName)
}
