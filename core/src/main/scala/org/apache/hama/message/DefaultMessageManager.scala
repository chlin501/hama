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

import java.io.IOException
import java.net.InetAddress
import java.net.InetSocketAddress
import java.util.ArrayList
import java.util.{ Iterator => Iter }
import java.util.Map.Entry

import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.io.Writable
import org.apache.hadoop.util.ReflectionUtils
import org.apache.hama.bsp.message.queue.DiskQueue
import org.apache.hama.bsp.TaskAttemptID
import org.apache.hama.fs.Operation
import org.apache.hama.fs.OperationFactory
import org.apache.hama.HamaConfiguration
import org.apache.hama.message.compress.BSPMessageCompressor
import org.apache.hama.message.compress.BSPMessageCompressorFactory
import org.apache.hama.message.queue.MemoryQueue
import org.apache.hama.message.queue.MessageQueue
import org.apache.hama.message.queue.SingleLockQueue
import org.apache.hama.message.queue.SynchronizedQueue
import org.apache.hama.util.LRUCache
import scala.collection.JavaConversions._

/**
 * Provide default functionality of {@link MessageManager}.
 */
class DefaultMessageManager[M <: Writable] extends MessageManager[M] {

  val LOG = LogFactory.getLog(classOf[DefaultMessageManager[M]])

  private var configuration: HamaConfiguration = _
  protected var taskAttemptId: TaskAttemptID = _
  protected var compressor: BSPMessageCompressor[M] = _
  protected var outgoingMessageManager: OutgoingMessageManager[M] = _
  protected var localQueue: MessageQueue[M] = _
  protected var localQueueForNextIteration: SynchronizedQueue[M] = _
  protected var maxCachedConnections: Int = 100
  /* This holds the reference to BSPPeer actors. */
  protected var peersLRUCache: LRUCache[PeerInfo, ActorRef] = _

  // TODO: create znodes so that we know where messages to go
  //       e.g. /bsp/messages/...
  override def init(conf: HamaConfiguration, taskAttemptId: TaskAttemptID) {
    this.taskAttemptId = taskAttemptId
    this.configuration = configuration
    //initializeCurator(configuration)
    this.localQueue = getReceiverQueue
    this.localQueueForNextIteration = getSynchronizedReceiverQueue
    this.compressor = BSPMessageCompressorFactory.getCompressor(configuration)
    this.outgoingMessageManager = getOutgoingMessageManager(compressor)
    this.maxCachedConnections = 
      conf.getInt("hama.messenger.max.cached.connections", 100) 
    this.peersLRUCache = initializeLRUCache(maxCachedConnections)
  }

  def initializeLRUCache(maxCachedConnections: Int): 
      LRUCache[PeerInfo,ActorRef] = {
    new LRUCache[PeerInfo, ActorRef](maxCachedConnections) {
      override def removeEldestEntry(eldest: Entry[PeerInfo, ActorRef]): 
          Boolean = {
        if (size() > this.capacity) {
          val peer = eldest.getKey
          remove(peer)
          return true
        }
        return false
      }
    }
  }

  /**
   * Memory queue doesn't perform any initialization after initialize() gets 
   * called.
   * @return MessageQueue type is backed with a particular queue implementation.
   */
  def getReceiverQueue: MessageQueue[M] = { 
    val queue: MessageQueue[M] = ReflectionUtils.newInstance( 
      configuration.getClass("hama.messenger.receive.queue.class", 
                             classOf[MemoryQueue[M]], classOf[MessageQueue[M]]),
      configuration
    ) 
    queue.init(configuration, taskAttemptId) 
    queue
  }

  def getSynchronizedReceiverQueue: SynchronizedQueue[M] = 
    SingleLockQueue.synchronize(getReceiverQueue)

  def getOutgoingMessageManager(compressor: BSPMessageCompressor[M]): 
      OutgoingMessageManager[M] = {
    val out = ReflectionUtils.newInstance(configuration.getClass(
                "hama.messenger.outgoing.message.manager.class",
                classOf[OutgoingPOJOMessageBundle[M]], 
                classOf[OutgoingMessageManager[M]]), configuration)
    out.init(configuration, compressor)
    out
  }

  override def close() {
    outgoingMessageManager.clear
    localQueue.close
    cleanupDiskQueue
  }

  def cleanupDiskQueue() {
    try {
      val operation = OperationFactory.get(this.configuration)
      val diskQueueDir = configuration.get("bsp.disk.queue.dir")
      operation.remove(DiskQueue.getQueueDir(configuration, 
                                             taskAttemptId,
                                             diskQueueDir))
    } catch {
      case e: IOException => 
        LOG.warn("Can't remove disk queue for "+taskAttemptId, e) 
    }
  }

  @throws(classOf[IOException])
  override def getCurrentMessage(): M = localQueue.poll
  
  override def getNumCurrentMessages(): Int = localQueue.size

  override def clearOutgoingMessages() {
    outgoingMessageManager.clear
    if (configuration.getBoolean("hama.queue.behaviour.persistent", false) && 
        localQueue.size > 0) { 
      if (localQueue.isMemoryBasedQueue &&
          localQueueForNextIteration.isMemoryBasedQueue) {
        // To reduce the number of element additions
        if (localQueue.size > localQueueForNextIteration.size) {
          localQueue.addAll(localQueueForNextIteration)
        } else {
          localQueueForNextIteration.addAll(localQueue)
          localQueue = localQueueForNextIteration.getMessageQueue
        }
      } else {
        // TODO find the way to switch disk-based queue efficiently.
        localQueueForNextIteration.addAll(localQueue)
        if (null != localQueue) {
          localQueue.close
        }
        localQueue = localQueueForNextIteration.getMessageQueue
      }
    } else {
      if (null != localQueue) {
        localQueue.close
      }
      localQueue = localQueueForNextIteration.getMessageQueue
    }
    localQueue.prepareRead
    localQueueForNextIteration = getSynchronizedReceiverQueue
  }

  @throws(classOf[IOException])
  override def send(peerName: String, msg: M) = {
    outgoingMessageManager.addMessage(peerName, msg);
    // TODO: increment counter by 1
    // peer.incrementCounter(BSPPeerImpl.PeerCounter.TOTAL_MESSAGES_SENT, 1L)
  }

  override def getOutgoingBundles(): 
      Iter[Entry[InetSocketAddress, BSPMessageBundle[M]]] = 
    outgoingMessageManager.getBundleIterator

/*
  @throws(classOf[IOException]) // TODO: remove this method?
  override def transfer(addr: InetSocketAddress, bundle: BSPMessageBundle[M]) {
    throw new UnsupportedOperationException("Not supported operation.")
  }
*/

  @throws(classOf[IOException])
  override def transfer(peer: PeerInfo, bundle: BSPMessageBundle[M]) {
/*
    mapAsScalaMap(peersLRUCache).find( entry => entry._1.equals(peer)) match { 
      case Some(found) => {
        peersLRUCache.get(found._2)
      }
      case None => {
        context.actorOf(Props())
        peersLRUCache.put(peer, )
      }
    }
*/
  }

/*
  // TODO: need util to help create actor path
  def actorPath(peer: PeerInfo): String = {
    //"akka.tcp://%s@%s:%d".format()
//peer.actorSystemName  
  }
*/

  @throws(classOf[IOException])
  override def loopBackMessages(bundle: BSPMessageBundle[M]) {}

  @throws(classOf[IOException])
  override def loopBackMessage(message: Writable) {} 

  override def listenerAddress(): InetSocketAddress = null

}
