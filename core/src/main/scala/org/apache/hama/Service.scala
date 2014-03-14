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
import akka.contrib.pattern.ReliableProxy
import org.apache.hama._
import scala.concurrent.duration._

/**
 * A service may hold a collection of services, and proxies link to a remote
 * server.
 */
// TODO: divide to Service (local), RemoteSerivce, ProxyService trait
trait Service extends Agent {

  /**
   * A fixed count of services expected to be available.
   */
  protected var servicesCount: Int = _

  /**
   * A service can hold other services e.g. plugin.
   */
  //TODO: change to set because ActorRef.path.name knows which actor is used.
  protected var services = Map.empty[String, ActorRef] 

  /**
   * Schedule to send messages to services, and cancel the scheduling once the
   * target service replies with Ack.
   */
  protected var cancelServicesWhenReady = Map.empty[String, Cancellable]

  /**
   * A fixed count of proxies expected to be available.
   */
  protected var proxiesCount: Int = _

  /**
   * A collection of {@link akka.contrib.pattern.ReliableProxy}s links to a 
   * remote server.
   */
  // TODO: change to set because ActorRef.path.name knows which actor is used.
  protected var proxies = Map.empty[String, ActorRef]

  /**
   * Cancel the scheduling once the proxy is ready to be used.
   */
  protected var cancelProxiesWhenReady = Map.empty[String, Cancellable]

  /**
   * Logic for intantiating necessary prerequisite operations.
   */
  def initializeServices = {}

  /**
   * A configuration file specific to Hama system.
   */
  def configuration: HamaConfiguration

  /**
   * Initialize either groom's or master's subservices.
   */
  override def preStart = initializeServices 
  
  /**
   * Create a service actor and schedule message checking if it's ready.
   * Another service must be in the same jvm as actor that creates the service,
   * so keep sending message instead of scheduleOnce.
   *
   * @param service is the name of the service actor.
   * @param target denotes the class name of that actor.
   */
  protected def create[A <: Actor](service: String, target: Class[A]) {
    // TODO: maybe use Option[HamaConfiguration] for null check
    val actor = context.actorOf(Props(target, configuration), service)
    import context.dispatcher
    val cancellable = 
      context.system.scheduler.schedule(0.seconds, 2.seconds, actor, 
                                        IsServiceReady)
    cancelServicesWhenReady ++= Map(service -> cancellable)
    LOG.debug("Services checker: {}", 
              cancelServicesWhenReady.keys.mkString(", "))
    servicesCount += 1
  }

  /**
   * Cache service to Service#services map.
   */
  protected def cacheService(name: String, service: ActorRef) {
    services ++= Map(name -> service)
    context.watch(service)
    cancelServicesChecker(name, service)
  }

  /**
   * Cancel sending IsServiceReady message when the service finishes loading.
   * This function will be executed in {@link ServiceStateMachine}.
   */
  protected def cancelServicesChecker(name: String, service: ActorRef) {
    cancelServicesWhenReady.get(name) match {
      case Some(cancellable) => cancellable.cancel
      case None =>
        LOG.warning("Can't cancel for service {} not found!", name)
    }
  }
  
  /**
   * Lookup a proxy actor by sending an {@link Identify} and schedule a message
   * indicating timeout if no reply.
   * When timeout, the actor needs to explicitly lookup again, so we use 
   * scheduleOnce instead of schedule function. This is due to proxy is inter-
   * jvm, which is different from create().
   *
   * @param target denotes the remote target actor name.
   * @param path indicate the path of target actor.
   */
  protected def lookup(target: String, path: String, 
                       timeout: FiniteDuration = 5.seconds) {
    if(null == proxies.getOrElse(target, null)) {
      context.system.actorSelection(path) ! Identify(path)
      import context.dispatcher
      val cancellable =
        context.system.scheduler.scheduleOnce(timeout, self, Timeout(target))
      cancelProxiesWhenReady ++= Map(target -> cancellable)
      proxiesCount += 1
    }
  }

  /**
   * Link to a particular remote proxy instance
   * @param target denotes the remote target actor name.
   * @param ref is the remote actor ref.
   */
  protected def link(target: String, ref: ActorRef) {
    LOG.debug("link to remote target: {} ref: {}.", target, ref)
    val proxy = context.system.actorOf(Props(classOf[ReliableProxy],
                                           ref,
                                           100.millis),
                                       target)
    proxies ++= Map(target -> proxy)
    cancelProxiesWhenReady.get(target) match {
      case Some(cancellable) => cancellable.cancel
      case None => 
        LOG.warning("Can't cancel for proxy {} not found!", target)
    }
    LOG.debug("Done linking to remote service {}.", target)
  }

  /**
   * Default mechanism in loading services by sending service name and its actor
   * reference.
   */
  protected def isServiceReady: Receive = {
    case IsServiceReady => {
      sender ! Load(name, self)
    }
  }

}
