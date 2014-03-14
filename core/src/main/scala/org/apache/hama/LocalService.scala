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

import akka.actor._
import akka.event._
import scala.concurrent.duration._

/**
 * A service that provides functions in looking up local services.
 */
trait LocalService extends Service {

  /**
   * A fixed count of local services expected to be available.
   */
  protected var servicesCount: Int = _

  /**
   * A service can hold other services.
   */
  protected var services = Set.empty[ActorRef] 

  /**
   * Schedule to send messages to services, and cancel the scheduling once the
   * target service replies.
   */
  protected var servicesLookup = Map.empty[String, Cancellable]

  /**
   * Create a service actor and schedule message checking if it's ready.
   * Another service must be in the same jvm as actor that creates the service,
   * so keep sending message instead of scheduleOnce.
   *
   * @param service is the name of the service actor.
   * @param target denotes the class name of that actor.
   */
  protected def create[A <: Actor](service: String, target: Class[A]) {
    val actor = context.actorOf(Props(target, configuration), service)
    import context.dispatcher
    val cancellable = 
      context.system.scheduler.schedule(0.seconds, 2.seconds, actor, 
                                        IsServiceReady)
    servicesLookup ++= Map(service -> cancellable)
    LOG.debug("Services to be created: {}", servicesLookup.keys.mkString(", "))
    servicesCount += 1
  }

  /**
   * Cache service to Service#services map.
   */
  protected def cacheService(service: ActorRef) {
    services ++= Set(service)
    context.watch(service)
    cancelServiceLookup(service.path.name, service)
  }

  /**
   * Cancel sending IsServiceReady message when the service finishes loading.
   * This function will be executed in {@link ServiceStateMachine}.
   * @param name is the key pointed to the service being looked up.
   * @param service holds reference to the service actor.
   */
  protected def cancelServiceLookup(name: String, service: ActorRef) {
    servicesLookup.get(name) match {
      case Some(cancellable) => {
        cancellable.cancel
        servicesLookup -= "name"
      }
      case None =>
        LOG.warning("Can't cancel for service {} not found!", name)
    }
  }

  protected def servicesReady: Boolean = (servicesCount == services.size)

  protected def offline(target: ActorRef) { }

  /**
   * Default mechanism in loading services by sending service name and its
   * actor reference.
   */
  protected def isServiceReady: Receive = {
    case IsServiceReady => sender ! Load
  }

  protected def areSubServicesReady: Receive = {
    case IsServiceReady => {
      if(servicesReady) 
        sender ! Load 
      else 
        LOG.info("Expected {} services, but only {} services are available.", 
                 servicesCount, services.size)
    }
  }

  protected def isTerminated: Receive = {
    case Terminated => offline(sender)
  }

}
