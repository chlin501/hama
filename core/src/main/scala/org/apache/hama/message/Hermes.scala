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
import org.apache.hama.RemoteService
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

  protected var actor: ActorRef = _

  private def getActor(): ActorRef = {
    if(null == this.actor) {
      val ctx = TypedActor.context
      this.actor = ctx.actorOf(Props(classOf[PeerMessenger]), "peerMessenger")
    }
    this.actor
  }

  override def initialize(conf: HamaConfiguration) = getActor ! Setup(conf)

  implicit val timeout = Timeout(30 seconds)

  override def transfer[M <: Writable](peer: PeerInfo, 
                                       msg: BSPMessageBundle[M]): 
      Future[TransferredState] = 
    (getActor ? Transfer[M](peer, msg)).mapTo[TransferredState]

}

final case class MsgFrom(msg: BSPMessageBundle[_ <: Writable], from: ActorRef)

/**
 * An messenger on behalf of {@link BSPPeer} sends messages to other peers.
 */
class PeerMessenger extends Actor with RemoteService {

  override val LOG = Logging(context.system, this)

  /* This holds information to BSPPeer actors. */
  protected var maxCachedConnections: Int = 100
  protected var peersLRUCache: LRUCache[PeerInfo, ActorRef] = _
  protected var initialized: Boolean = false
  protected var conf: HamaConfiguration = new HamaConfiguration() 

  protected var waitingList = Map.empty[PeerInfo, MsgFrom]

  override def configuration(): HamaConfiguration = this.conf

  def initializeService(conf: HamaConfiguration) {
    this.conf = conf
    this.maxCachedConnections =
      this.conf.getInt("hama.messenger.max.cached.connections", 100)
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

  def remotePeerAddress(peer: PeerInfo): String = 
     peer.path()+"/user/peerMessenger"
 
  /**
   * Cache message bundle and {@link BSPPeer} in waiting list.
   * Lookup corresponded remote {@link BSPPeer}'s PeerMessenger.
   * @param peer is the remote PeerMessenger actor reference.
   * @param msg is the message to be sent.
   * @param from is the bsp peer who issues the transfer request.
   */
  def findWith[M <: Writable](peer: PeerInfo, msg: BSPMessageBundle[M],
                              from: ActorRef) {
    waitingList ++= Map(peer -> MsgFrom(msg, from)) 
    LOG.info("Look up remote peer "+peer.path())
    lookup(peer.path(), remotePeerAddress(peer)) 
  }

  override def afterLinked(target: String, proxy: ActorRef) {
    waitingList.find(entry => entry._1.path.equals(target)) match {
      case Some(found) => {
        val msgFrom = found._2
        val msg = msgFrom.msg
        val from = msgFrom.from
        LOG.debug("Transfer message to {} with size {}", 
                  target, msg.size)
        proxy ! msg
        confirm(from)
      }
      case None => LOG.warning("No corresponded msg can be sent to {}", target)
    }
  } 

  /**
   * Confirm that message bundle is sent to remote {@link BSPPeer}s.
   * @param from denotes the local BSPPeers that issues transfer request.
   */
  def confirm(from: ActorRef) = from ! TransferredCompleted 

  /**
   * Transfer message to peers. If peer is not found in cache, then lookup 
   * first, and transfer when remote peer is obtained.
   */
  def transfer: Receive = {
    case Transfer(peer, msg) => {
      if(initialized) {
        mapAsScalaMap(peersLRUCache).find( 
          entry => entry._1.equals(peer)
        ) match {
          case Some(found) => {
            val proxy = peersLRUCache.get(found._2) 
            proxy ! msg
            val from = sender
            confirm(from)
          }
          case None => {
            // seender is a bsp peer who sends transfer request
            val from = sender 
            findWith(peer, msg, from)
          }
        }
      } else {
        sender ! MessengerUninitialized 
      }
    }
  }

  override def receive = initialize orElse transfer orElse unknown

}
