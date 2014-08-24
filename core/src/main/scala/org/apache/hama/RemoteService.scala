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
import akka.actor.ActorIdentity
import akka.actor.Cancellable
import akka.actor.Identify
import akka.actor.Props
import akka.contrib.pattern.ReliableProxy
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration.DurationInt

/**
 * A service that provides remote methods such as proxy lookup.
 */
trait RemoteService extends Service {

  /**
   * A fixed count of proxies expected to be available.
   */
  protected var proxiesCount: Int = _

  /**
   * A collection of {@link akka.contrib.pattern.ReliableProxy}s links to a 
   * remote server.
   */
  protected var proxies = Set.empty[ActorRef]

  /**
   * Cancel the scheduling once the proxy is ready to be used.
   */
  protected var proxiesLookup = Map.empty[String, Cancellable]

  /**
   * Lookup a proxy actor by sending an {@link Identify} and schedule a message
   * indicating timeout if no reply.
   * When timeout, the actor needs to explicitly lookup again, so we use
   * scheduleOnce instead of schedule function. This is due to proxy is inter-
   * jvm, which is different from getOrCreate().
   *
   * @param target denotes the remote target actor name.
   * @param path indicate the path of target actor.
   */
  protected def lookup(target: String, path: String,
                       timeout: FiniteDuration = 5.seconds) {
    proxies.find(p => p.path.name.equals(target)) match {
      case Some(found) => {
        proxies -= found 
        refreshProxy(target, path)
      }
      case None => {
        refreshProxy(target, path)
        proxiesCount += 1
      }
    }
  }

  private def refreshProxy(target: String, path: String, 
                           timeout: FiniteDuration = 5.seconds) {
    LOG.debug("Lookup proxy {} at {}", target, path)
    context.system.actorSelection(path) ! Identify(target)
    import context.dispatcher
    val cancellable =
      context.system.scheduler.scheduleOnce(timeout, self, 
                                            Timeout(target, path))
    proxiesLookup ++= Map(target -> cancellable)
    LOG.debug("Proxies to be lookup? {}", proxiesLookup.mkString(", "))
  }

  /**
   * Link to a particular remote proxy instance
   * @param target denotes the remote target actor name.
   * @param ref is the remote actor ref.
   */
  protected def link(target: String, ref: ActorRef): ActorRef = {
    LOG.debug("Link to remote target: {} ref: {}.", target, ref)
    val proxy = context.system.actorOf(Props(classOf[ReliableProxy],
                                           ref,
                                           100.millis),
                                       target)
    proxies ++= Set(proxy)
    proxiesLookup.get(target) match {
      case Some(cancellable) => cancellable.cancel
      case None =>
        LOG.warning("Can't cancel for proxy {} not found!", target)
    }
    LOG.debug("Done linking to remote service {}.", target)
    proxy
  }

  /**
   * Post process once the remote actor is linked.
   * @param proxy is the remote actor linked via {@link RemoteService#lookup}.
   */
  protected def afterLinked(proxy: ActorRef) {}

  
  /**
   * Post process once the remote actor is linked.
   * @param target is the name used in {@link #lookup}.
   * @param proxy is the remote actor linked via {@link RemoteService#lookup}.
   */
  protected def afterLinked(target: String, proxy: ActorRef) {}

  /**
   * Provide a way to transform the linked actor reference.
   * @param target is the name of the remote actor.
   * @param proxy is the remote actor reference to be used.
   * @return (String, ActorRef) as tuple where the formor is the name of the 
   *                            actor and the latter is the remote reference.
  protected def beforeLinked(target: String, proxy: ActorRef): 
      (String, ActorRef) = (target, proxy)
   */

  override protected def remoteReply(target: String, remote: ActorRef) {
    LOG.info("Proxy {} is ready: {}", target, remote)
    context.watch(remote) // TODO: watch proxy instead?
    val proxy = link(target, remote)//remote.path.name
    afterLinked(remote) // TODO: need to switch using proxy ReliableProxy
    afterLinked(target, remote)
  }

  /**
   * A reply from the remote actor indicating if the remote actor is ready to 
   * provide its service.
   * @param Receive is paritual function.
  protected def isProxyReady: Receive = {
    case ActorIdentity(target, Some(remote)) => {
      LOG.info("Proxy {} is ready.", target)
      context.watch(remote) // TODO: watch proxy instead?
      val pair = beforeLinked(target.asInstanceOf[String], remote)
      val newName = pair._1
      val newActor = pair._2
      if(null == newName || null == newActor) 
        throw new RuntimeException("Calling beforeLinked function leads to "+
                                  "actor ref and its name missing!")
      val proxy = link(newName, newActor)//remote.path.name 
      afterLinked(newActor) // TODO: need to switch using proxy
      afterLinked(newName, newActor)
    }
    case ActorIdentity(target, None) => 
      LOG.warning("Proxy {} is not yet available!", target)
  }
   */

  /**
   * Timeout reply when looking up a specific remote proxy.
   * @param Receive is partial function.
   */
  protected def timeout: Receive = {
    case Timeout(proxy, path) => {
      LOG.debug("Timeout when looking up proxy {} ", proxy)
      lookup(proxy, path)
    }
  }

}
