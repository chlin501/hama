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
import akka.actor.ActorIdentity
import akka.actor.Props
import org.apache.hama.logging.ActorLog

trait Agent extends Actor with ActorLog {

  /**
   * The name for this actor. 
   * @return String is the name of this actor when created.
   */
  final def name(): String = self.path.name

  protected def unknown: Receive = {
    case msg@_ => {
      LOG.warning("Unknown message {} for {}.", msg, name)
    }
  }
 
  /**
   * Remote actor reply to actor selection.
   * @param target denotes the name of the <b>remote</b> actor reference.
   * @param actor is the instance of ActoRef pointed to a <b>remote</b> actor.
   */
  protected def remoteReply(target: String, actor: ActorRef) { }
  
  /**
   * Local actor reply to actorSelection.  
   * @param target denotes the name of the actor reference.
   * @param actor is the instance of ActoRef pointed to a local actor.
  protected def localReply(target: String, actor: ActorRef) { }
   */

  /**
   * Another actor reply the query of actorSelection, either local or remote.
   * @param target is the value of Identify when performing actorSelection.
   * @param actor is the instance of ActoRef.
   */
  protected def actorReply: Receive = {
    case ActorIdentity(target, Some(actor)) => {
      remoteReply(target.toString, actor)
      //localReply(target, actor)
    }
    case ActorIdentity(target, None) => LOG.warning("{} is not yet available!",
                                                    target)
  }

  /**
   * Spawn a child actor.
   * @param childName is the name of child actor. 
   * @param actorClass is the actor class to be spawned.
   * @param args is variable args to be used by the actor.
   * @return ActorRef is the created actor instance.
   */
  protected def spawn[A <: Actor](childName: String, actorClass: Class[A],
                                  args: Any*): ActorRef = {
    LOG.debug("Spawn child {} actor as {} with args {}", 
              actorClass, childName, args.mkString(", "))
    context.actorOf(Props(actorClass, args:_*), childName)
  }
}
