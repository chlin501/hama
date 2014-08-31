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

//import akka.actor.Actor
import akka.actor.ActorRef
//import akka.actor.ActorSystem
import akka.actor.TypedActor
//import akka.event.Logging
//import akka.pattern.ask
import akka.util.Timeout
import java.net.InetAddress
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import java.util.Map.Entry
import org.apache.hadoop.io.Writable
import org.apache.hama.bsp.v2.LocalMessages
import org.apache.hama.HamaConfiguration
import org.apache.hama.ProxyInfo
import org.apache.hama.RemoteService
import org.apache.hama.util.LRUCache
import org.apache.hama.util.Utils._
import scala.collection.JavaConversions._
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration
//import scala.concurrent.Future

/**
 * Denote to initialize messenger's services for transfer messages over wire. 
 * @param conf is common configuration.
// TODO: remove blocking queue for we don't use typed actor any more.
final case class Setup[M <: Writable](conf: HamaConfiguration, 
                                      q: BlockingQueue[BSPMessageBundle[M]])
 */

/**
 * Indicate to which target local messages will be sent.  
final case class LocalTarget(worker: ActorRef) 
 */

/**
 * An object that contains peer and message bundle. The bundle will be sent 
 * to peer accordingly.
 * @param peer is the destination to which will be sent.
 * @param msg is the actual data.
 */
final case class Transfer[M <: Writable](peer: ProxyInfo, msg: BSPMessageBundle[M])

final case class MessageFrom(msg: BSPMessageBundle[_ <: Writable], 
                             from: ActorRef)

object PeerMessenger {

  val loopbackQueue = new LinkedBlockingQueue[BSPMessageBundle[_]]()

}

/**
 * An messenger on behalf of {@link BSPPeer} sends messages to other peers.
 */
class PeerMessenger(conf: HamaConfiguration) extends RemoteService {

  import PeerMessenger._

  /* This holds information to BSPPeer actors. */
  protected val maxCachedConnections: Int = 
    conf.getInt("hama.messenger.max.cached.connections", 100)
  protected var peersLRUCache = initializeLRUCache(maxCachedConnections)
  //protected var initialized: Boolean = false
  //protected var conf: HamaConfiguration = new HamaConfiguration() 
  //protected var loopbackQueue: BlockingQueue[BSPMessageBundle[_]] = _  // TODO: remove this one! for we've removed TypedActor
 
  /**
   * Peer may not be available immediately, so store it in waiting list first.
   */
  protected var waitingList = Map.empty[ProxyInfo, MessageFrom]

  override def configuration(): HamaConfiguration = this.conf

/*
  def initializeService[M <: Writable](conf: HamaConfiguration, 
                                       q: BlockingQueue[BSPMessageBundle[M]]) {
    this.conf = conf
    if(null == q)
      throw new RuntimeException("Loopback message queue is empty!") 
    loopbackQueue = q.asInstanceOf[BlockingQueue[BSPMessageBundle[_]]]

    this.maxCachedConnections =
      this.conf.getInt("hama.messenger.max.cached.connections", 100)
    this.peersLRUCache = initializeLRUCache(maxCachedConnections)
    this.initialized = true
  }
*/

  protected def initializeLRUCache(maxCachedConnections: Int):
      LRUCache[ProxyInfo,ActorRef] = {
    new LRUCache[ProxyInfo, ActorRef](maxCachedConnections) {
      override def removeEldestEntry(eldest: Entry[ProxyInfo, ActorRef]):
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

/*
  def initialize: Receive = {
    case Setup(conf, q) => initializeService(conf, q)
  }
*/
 
  /**
   * Cache message bundle and {@link BSPPeer} in waiting list.
   * Lookup corresponded remote {@link BSPPeer}'s PeerMessenger.
   * @param peer is the remote PeerMessenger actor reference.
   * @param msg is the message to be sent.
   * @param from is the bsp peer who issues the transfer request.
   */
  protected def findWith[M <: Writable](peer: ProxyInfo, 
                                        msgs: BSPMessageBundle[M],
                                        from: ActorRef) = msgs match {
    case null => LOG.warning("Messages for {} not found", peer)
    case bundle@_ => from match {
      case null =>
      case f@_ => {
        addToWaitingList(peer, MessageFrom(msgs, f))
        LOG.info("Look up remote peer "+peer.getActorName+" at "+peer.getPath)
        lookupPeer(peer.getActorName, peer.getPath)     
      }
    }
  }

  protected def addToWaitingList(peer: ProxyInfo, msgFrom: MessageFrom) =
    waitingList ++= Map(peer -> msgFrom) 

  protected def lookupPeer(name: String, addr: String) = lookup(name, addr) 

  protected def cache(peer: ProxyInfo, proxy: ActorRef) = 
    peersLRUCache.put(peer, proxy)   

  override def afterLinked(target: String, proxy: ActorRef) = 
    findThenSend(target, proxy) 

  protected def findThenSend(target: String, proxy: ActorRef) {
    waitingList.find(entry => entry._1.getActorName.equals(target)) match {
      case Some(found) => {
        val msgFrom = found._2
        val msg = msgFrom.msg 
        val from = msgFrom.from
        cache(found._1, proxy)
        LOG.info("Transfer message to {} with size {}", target, msg.size)
        proxy ! msg
        confirm(from)
      }
      case None => LOG.warning("Target {} for sending message bundle.",
                               target)
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
    case Transfer(peer, bundle) => doTransfer(peer, bundle, sender)
  }

  protected def doTransfer[M <: Writable](peer: ProxyInfo, 
                                          bundle: BSPMessageBundle[M], 
                                          from: ActorRef) {
    //if(initialized) {
      mapAsScalaMap(peersLRUCache).find( 
        entry => entry._1.equals(peer)
      ) match {
        case Some(found) => {
          val proxy = peersLRUCache.get(found._2) 
          proxy ! bundle  
          confirm(from)
        }
        case None => findWith(peer, bundle, from)
      }
/*
    } else {
      LOG.warning("PeerMessenger is not initialized!") 
      from ! MessengerUninitialized 
    }
*/
  }

  /**
   * A {@link PeerMessenger} may receive messages bundle from remote peer 
   * messenger. Once it receives a message bundle, this method gets called, and
   * it puts the bundle to the queue that in another thread in turns retrieves
   * by calling {@link MessageManager#loopBackMessages}.
   */
  def messageFromRemote: Receive = {
    case bundle: BSPMessageBundle[_] => {
      LOG.info("Message received from {} is putting to loopback queue!", 
               sender)
      loopbackQueue.put(bundle) 
    }
  }

  override def receive = transfer orElse messageFromRemote orElse actorReply orElse timeout orElse unknown

}
