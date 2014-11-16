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
  protected[hama] def unloadService[A <: Actor](serviceName: String): 
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
