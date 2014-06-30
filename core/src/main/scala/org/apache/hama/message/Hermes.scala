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
package org.apache.hama.message

import akka.actor.Actor
import akka.actor.ActorContext
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.TypedActor
import akka.event.Logging
import akka.pattern.ask
import akka.util.Timeout
import java.util.Map.Entry
import org.apache.hadoop.io.Writable
import org.apache.hama.HamaConfiguration
import org.apache.hama.util.LRUCache
import scala.collection.JavaConversions._
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.Future

/**
 * Denote to initialize messenger's services for transfer messages over wire. 
 * @param conf is common configuration.
 */
final case class Setup(conf: HamaConfiguration)

/**
 * An object that contains peer and message bundle. The bundle will be sent 
 * to peer accordingly.
 * @param peer is the destination to which will be sent.
 * @param msg is the actual data.
 */
final case class Transfer[M <: Writable](peer: PeerInfo, msg: BSPMessageBundle[M])

/**
 * A bridge for BSPPeer and message transfer actor.
 */
trait Hermes {

  /**
   * Initialize necessary services with common configuration.
   * @param conf is common setting from container.
   */
  def initialize(conf: HamaConfiguration)

  /**
   * A function transfer message bundle to another bsp peer.
   * @param is destination bsp peer.
   * @param msg contains messages to be transferred to the destination.
   */
  def transfer[M <: Writable](peer: PeerInfo, msg: BSPMessageBundle[M]): 
    Future[TransferredState]

}

class Iris extends Hermes {

  protected val actor = {
    val ctx = TypedActor.context
    ctx.actorOf(Props(classOf[PeerMessenger]))
  }

  override def initialize(conf: HamaConfiguration) {
    actor ! Setup(conf)
  }

  implicit val timeout = Timeout(30 seconds)

  override def transfer[M <: Writable](peer: PeerInfo, 
                                       msg: BSPMessageBundle[M]): 
      Future[TransferredState] = {
    (actor ? Transfer[M](peer, msg)).mapTo[TransferredState]
  }

}

/**
 * An messenger on behalf of {@link BSPPeer} sends messages to other peers.
 */
class PeerMessenger extends Actor {

  val LOG = Logging(context.system, this)
  /* This holds information to BSPPeer actors. */
  protected var maxCachedConnections: Int = 100
  protected var peersLRUCache: LRUCache[PeerInfo, ActorRef] = _
  protected var initialized: Boolean = false

  protected def initializeService(conf: HamaConfiguration) {
    this.maxCachedConnections =
      conf.getInt("hama.messenger.max.cached.connections", 100)
    this.peersLRUCache = initializeLRUCache(maxCachedConnections)
    this.initialized = true
  }

  protected def initializeLRUCache(maxCachedConnections: Int):
      LRUCache[PeerInfo,ActorRef] = {
    new LRUCache[PeerInfo, ActorRef](maxCachedConnections) {
      override def removeEldestEntry(eldest: Entry[PeerInfo, ActorRef]):
          Boolean = {
        if (size() > this.capacity) {
          val peer = eldest.getKey
          remove(peer)
          true
        }
        false
      }
    }
  }


  def initialize: Receive = {
    case Setup(conf) => initializeService(conf)
  }

  /**
   * Transfer message to peers. If peer is not found in cache, then lookup 
   * first, and transfer when remote peer is obtained.
   */
  def transfer: Receive = {
    case Transfer(peer, msg) => {
      if(!initialized)  // TODO: change to either
        throw new IllegalStateException("PeerMessenger is not initialzied!")
/*
      mapAsScalaMap(peersLRUCache).find( entry => entry._1.equals(peer)) match {
        case Some(found) => {
          //peersLRUCache.get(found._2)
        }
        case None => // TODO: lookup remote actor and then send over 
                     // wire if found e.g. lookup(peer, msg, locate())
                     // once transferred completed call 
                     // sender ! TransferredCompleted
      }
*/
      //sender ! TransferredCompleted  // TODO: need to change waiting for all msg are sent
    // check if peer exists?
      // if not, book and then lookup. once linked, transfer message.
      // if all message are sent, reply AllMessagesSent object, indicating 
      // transfer() is done.
    }
  }

  def unknown: Receive = {
    case msg@_ => LOG.warning("Unknown message {} received by {}", 
                              msg, self.path.name)
  }

  override def receive = initialize orElse transfer orElse unknown

}
