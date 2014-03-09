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
trait Service extends Agent {

  /**
   * A fixed count of services expected to be available.
   */
  protected var servicesCount: Int = _

  /**
   * A service can hold other services e.g. plugin.
   */
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
  protected var proxies = Map.empty[String, ActorRef]

  /**
   * Cancel the scheduling once the proxy is ready to be used.
   */
  protected var cancelProxiesWhenReady = Map.empty[String, Cancellable]

  /**
   * Logic for intantiating necessary prerequisite operations.
   */
  def initializeServices = {}

  def configuration: HamaConfiguration

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
    servicesCount += 1
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
  }

  /**
   * An external service acks so the current service caches and cancels 
   * scheduling {@link Timeout} messages. 
  protected def ack: Receive = {
    case Ack(service) => {
      LOG.info("Service {} is ready.", service)
      services ++= Map(service -> sender)
      context.watch(sender)
      cancelServicesWhenReady.get(service) match {
        case Some(cancellable) => cancellable.cancel
        case None => 
          LOG.warning("Can't cancel for service {} not found!", service)
      }
    }
  }
   */

  /**
   * Default replying mechanism.
   * Upon reception of Ready message, ack back with this service name.
   */
  protected def ready: Receive = { case Ready => sender ! Ack(name) }
  
}
