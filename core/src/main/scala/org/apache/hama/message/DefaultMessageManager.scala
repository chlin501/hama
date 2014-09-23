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
import org.apache.hama.Service
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
      extends MessageManager[M] with Service with TaskLog with MessageView {

  protected val outgoingMessageManager = OutgoingMessageManager.get[M](conf)
  protected val localQueue = getReceiverQueue

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
  
  override def getNumCurrentMessages(): Int = localQueue.size 
  
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

  override def getOutgoingBundles(): 
    Iter[java.util.Map.Entry[ProxyInfo, BSPMessageBundle[M]]] = 
    outgoingMessageManager.getBundleIterator

  /**
   * Actual transfer messsages over wire.
   * It first finds the peer and then send messages.
   * @param peer contains information of another peer. 
   * @param bundle are messages to be sent.
   */
  @throws(classOf[IOException])
  override def transfer(peer: ProxyInfo, bundle: BSPMessageBundle[M]) = {
/* TODO: merge peerMessenger with DefaultMessageManager
    peerMessenger match {
      case Some(found) => found ! Transfer(peer, bundle)
      case None =>
    }
*/
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

  override def receive = unknown  // TODO: not yet finish

}
