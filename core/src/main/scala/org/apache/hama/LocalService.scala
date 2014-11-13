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
import akka.actor.Cancellable
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration.DurationInt

final case object MediatorIsUp

/**
 * A service that provides functions in creating local services. 
 * Local services communication relies on mediator, either master or groom
 * server.
 */
trait LocalService extends Service {

  /**
   * Services created by this service.
   */
  protected var services = Set.empty[ActorRef] 

  protected var requestCache = Map.empty[String, Cancellable]

  /**
   * Request with message sent to a particular actor.
   * @param target is the actor to which the message will be routed.
   * @param message denotes what will be consumed by the target.
   * @param initial is the time the scheduler will start sending message. 
   * @param delay is the time the scheduler will wait for sending next message. 
   */
  protected def request(target: ActorRef, message: Any, 
                        initial: FiniteDuration = 0.seconds,
                        delay: FiniteDuration = 3.seconds): Cancellable = {
    LOG.debug("Request message {} to target: {}", message, target)
    import context.dispatcher
    val cancellable = context.system.scheduler.schedule(initial, delay, target, 
                                                        message)
    cacheRequest(message.toString, cancellable)
    cancellable
  }

  protected def cacheRequest(key: String, toBeCancelled: Cancellable) = 
    requestCache ++= Map(key -> toBeCancelled)

  protected def cancelRequest(key: String) {
    requestCache.get(key).map { (v) => v.cancel }
    requestCache -= key
  }
  
  protected[hama] def getOrCreate[A <: Actor](serviceName: String, 
                                              target: Class[A],
                                              args: Any*): ActorRef = 
    services.find( service => service.path.name.equals(serviceName) ) match {
      case Some(found) => found
      case None => {
        val actor = context.actorOf(Props(target, args:_*), serviceName)
        context.watch(actor)
        services ++= Set(actor)
        actor
      }
    }

  /**
   * Unload a particular service created by this one.
   * @param serviceName to be unloaded.
   */
  protected[hama] def unload[A <: Actor](serviceName: String): 
    Option[ActorRef] = services.find( service => 
      service.path.name.equals(serviceName)
    ) match {
      case Some(found) => {
        services -= found 
        context.unwatch(found) 
        Some(found)
      }
      case None => None
    }

}
