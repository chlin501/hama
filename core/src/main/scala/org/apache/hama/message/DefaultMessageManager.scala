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

import akka.actor.ActorRef
import akka.actor.ActorSystem
import java.io.IOException
import java.net.InetAddress
import java.net.InetSocketAddress
import java.util.ArrayList
import java.util.{ Iterator => Iter }
import java.util.Map.Entry
import org.apache.hadoop.io.Writable
import org.apache.hama.HamaConfiguration
import org.apache.hama.ProxyInfo
import org.apache.hama.RemoteService
import org.apache.hama.bsp.TaskAttemptID
import org.apache.hama.fs.Operation
import org.apache.hama.logging.Logging
import org.apache.hama.logging.LoggingAdapter
import org.apache.hama.logging.TaskLog
import org.apache.hama.logging.TaskLogger
import org.apache.hama.logging.TaskLogging
import org.apache.hama.message.compress.BSPMessageCompressor
import org.apache.hama.message.queue.MessageQueue
import org.apache.hama.message.queue.SingleLockQueue
import org.apache.hama.message.queue.SynchronizedQueue
import org.apache.hama.message.queue.Viewable
import org.apache.hama.util.LRUCache
import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.{Try, Failure, Success}

sealed trait MessengerMessage
final case class Send(peerName: String, msg: Writable) extends MessengerMessage
final case object GetCurrentMessage extends MessengerMessage
final case object GetNumCurrentMessages extends MessengerMessage
final case object GetOutgoingBundles extends MessengerMessage
final case object ClearOutgoingMessages extends MessengerMessage
final case object GetListenerAddress extends MessengerMessage

final protected[message] case class MessageFrom(
  msg: BSPMessageBundle[_ <: Writable], from: ActorRef
)

/**
 * Provide default functionality of {@link MessageManager}.
 * It realizes message communication by java object, and send messages through
 * actor via {@link akka.actor.TypedActor}.
 * @param conf is the common configuration, not specific for task.
 */
class DefaultMessageManager[M <: Writable](conf: HamaConfiguration,
                                           slotSeq: Int,
                                           taskAttemptId: TaskAttemptID,
                                           tasklog: ActorRef)
      extends MessageManager[M] with RemoteService with TaskLog 
      with MessageView {

  protected val outgoingMessageManager = OutgoingMessageManager.get[M](conf)
  protected val localQueue = getReceiverQueue
  protected val maxCachedConnections = 
    conf.getInt("hama.messenger.max.cached.connections", 100)
  protected val peersLRUCache = initializeLRUCache(maxCachedConnections)
  protected var waitingList = Map.empty[ProxyInfo, MessageFrom]

  override def LOG: LoggingAdapter = Logging[TaskLogger](tasklog)

  override def stopServices() = close

  override def configuration(): HamaConfiguration = conf

  /**
   * Indicate the local peer.
   */
  protected def currentPeer(conf: HamaConfiguration): ProxyInfo = {
    val host = conf.get("bsp.peer.hostname", 
                        InetAddress.getLocalHost.getHostName) 
    val port = conf.getInt("bsp.peer.port", 61000)
    val addr = "BSPPeerSystem%d@%s:%d".format(slotSeq, host, port)
    LOG.debug("Current peer address is "+addr)
    Peer.at(addr)
  }

  /**
   * Default implementation is {@link MemoryQueue}.
   * Memory queue doesn't perform any initialization after init() gets called.
   * @return MessageQueue type is backed with a particular queue implementation.
   */
  protected def getReceiverQueue: MessageQueue[M] = { 
    val queue = MessageQueue.get[M](conf)
    queue.init(conf, taskAttemptId)
    queue
  }

  override def close() {
    outgoingMessageManager.clear
    localQueue.close  
  }

  @throws(classOf[IOException])
  override def getCurrentMessage(): M = localQueue.poll 

  protected def currentMessage: Receive = {
    case GetCurrentMessage => sender ! getCurrentMessage
  }
  
  override def getNumCurrentMessages(): Int = localQueue.size 

  protected def numberCurrentMessages: Receive = {
    case GetNumCurrentMessages => sender ! getNumCurrentMessages
  }

  protected def clear: Receive = {
    case ClearOutgoingMessages => clearOutgoingMessages
  } 
  
  override def clearOutgoingMessages() = {
    outgoingMessageManager.clear
    localQueue.close
    localQueue.prepareRead
  }

  override def localMessages[M](): Option[List[M]] = 
    localQueue.isInstanceOf[Viewable[M]] match {
      case true => Option(localQueue.asInstanceOf[Viewable[M]].view.toList)
      case false => None
    }

  /**
   * When the client calls this function, following actions are taken place: 
   * - lookup remote ActorRef 
   * - once received reply, adding ActorRef to LRUCache.
   */
  // TODO: report stats
  @throws(classOf[IOException])
  override def send(peerName: String, msg: M) = 
    outgoingMessageManager.addMessage(Peer.at(peerName), msg); 

  protected def sendMessage: Receive = {
    case Send(peerName, msg) => send(peerName, msg.asInstanceOf[M])
  }

  // TODO: refactor methods return types
  override def getOutgoingBundles(): 
    Iter[java.util.Map.Entry[ProxyInfo, BSPMessageBundle[M]]] = 
    outgoingMessageManager.getBundleIterator

  protected def outgoingBundles: Receive = {
    case GetOutgoingBundles => {
      sender ! asScalaIterator(getOutgoingBundles).map( v => 
        (v.getKey, v.getValue)
      ) 
    }
  }

  @throws(classOf[IOException]) 
  override def transfer(peer: ProxyInfo, bundle: BSPMessageBundle[M]) = 
    LOG.warning("This function, executed by {}, doesn't take effect!", 
                Thread.currentThread.getName)

  /**
   * Client can use ask pattern for synchronous execution (blocking call).
   */
  protected def transferMessages: Receive = {
    case Transfer(peer, bundle) => 
      transfer(peer, bundle.asInstanceOf[BSPMessageBundle[M]], sender)
  }

  /**
   * Actual transfer messsages over wire.
   * It first finds the peer and then send messages.
   * @param peer contains information of another peer. 
   * @param bundle are messages to be sent.
   */  @throws(classOf[IOException]) 
  override def transfer(peer: ProxyInfo, bundle: BSPMessageBundle[M], 
                        from: ActorRef) {
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
  }

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

  def confirm(from: ActorRef) = from ! TransferredCompleted 

  protected def findWith[M <: Writable](peer: ProxyInfo, 
                                        msgs: BSPMessageBundle[M],
                                        from: ActorRef) = msgs match {
    case null => LOG.warning("Messages for {} is empty!", peer)
    case bundle@_ => from match {
      case null => LOG.warning("Unknown sender for {} ", peer)
      case f@_ => {
        addToWaitingList(peer, MessageFrom(msgs, f))
        LOG.info("Look up remote peer "+peer.getActorName+" at "+peer.getPath)
        lookupPeer(peer.getActorName, peer.getPath)     
      }
    }
  }

  protected def addToWaitingList(peer: ProxyInfo, msgFrom: MessageFrom) =
    waitingList ++= Map(peer -> msgFrom) 

  protected def removeFromWaitingList(peer: ProxyInfo) = waitingList -= peer

  protected def lookupPeer(name: String, addr: String) = lookup(name, addr)

  override def afterLinked(target: String, proxy: ActorRef) = 
    findThenSend(target, proxy) 

  /**
   * Find the peer name equals to the target, and then send the bundler over
   * network.
   * @param target of peer actor name.
   * @param proxy is the remote MessageManager actor reference.
   */
  protected def findThenSend(target: String, proxy: ActorRef) {
    waitingList.find(entry => {
      val proxyInfo = entry._1
      proxyInfo.getActorName.equals(target)
    }) match {
      case Some(found) => {
        val msgFrom = found._2
        val msg = msgFrom.msg 
        val from = msgFrom.from
        cache(found._1, proxy)
        LOG.debug("Transfer message to {} with size {}", target, msg.size)
        proxy ! msg
        removeFromWaitingList(found._1)
        confirm(from)
      }
      case None => LOG.warning("{} for sending message bundle not found!",
                               target)
    }
  }

  protected def cache(peer: ProxyInfo, proxy: ActorRef) = 
    peersLRUCache.put(peer, proxy)  

  protected def putMessagesToLocal: Receive = {
    case bundle: BSPMessageBundle[M] => {
      LOG.info("Message received from {} is putting to local queue!", 
               sender)
      loopBackMessages(bundle) 
    }
  }

  @throws(classOf[IOException])
  override def loopBackMessages(bundle: BSPMessageBundle[M]) = {
    val threshold = BSPMessageCompressor.threshold(Option(conf))
    bundle.setCompressor(BSPMessageCompressor.get(conf), threshold)
    val it: Iter[_ <: Writable] = bundle.iterator
    while (it.hasNext) loopBackMessage(it.next)
  }

  // TODO: report stats
  @throws(classOf[IOException])
  override def loopBackMessage(message: Writable) {
    localQueue.add(message.asInstanceOf[M])
  } 

  override def getListenerAddress(): ProxyInfo = currentPeer(conf)

  protected def listenerAddress: Receive = {
    case GetListenerAddress => sender ! getListenerAddress
  }

  override def receive = sendMessage orElse currentMessage orElse numberCurrentMessages orElse outgoingBundles orElse transferMessages orElse clear orElse putMessagesToLocal orElse listenerAddress orElse actorReply orElse timeout orElse superviseeIsTerminated orElse unknown 

}
