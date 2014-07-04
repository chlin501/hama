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
import akka.actor.TypedActor
import akka.actor.TypedProps
import java.io.IOException
import java.net.InetAddress
import java.net.InetSocketAddress
import java.util.ArrayList
import java.util.{ Iterator => Iter }
import java.util.Map.Entry
import org.apache.hadoop.conf.Configurable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Writable
import org.apache.hama.bsp.message.queue.DiskQueue
import org.apache.hama.bsp.TaskAttemptID
import org.apache.hama.fs.Operation
import org.apache.hama.HamaConfiguration
import org.apache.hama.message.queue.MemoryQueue
import org.apache.hama.message.queue.MessageQueue
import org.apache.hama.message.queue.SingleLockQueue
import org.apache.hama.message.queue.SynchronizedQueue
import org.apache.hama.util.LRUCache
import org.apache.hama.logging.Logger
import scala.collection.JavaConversions._

/**
 * A bridge device enables bsp peers communication through TypedActor.
 */
trait PeerCommunicator {
  
  /**
   * Initialize setting in allowing MessageManager to communicate with other 
   * {BSPPeer}s through TypedActor.
   */
  def initialize(sys: ActorSystem)

}

/**
 * Provide default functionality of {@link MessageManager}.
 * It realizes message communication by java object, and send messages through
 * actor via {@link akka.actor.TypedActor}.
 * @param conf is the common configuration, not specific for task.
 */
class DefaultMessageManager[M <: Writable] extends MessageManager[M] 
                                           with PeerCommunicator
                                           with Logger {

  protected var configuration: HamaConfiguration = _
  protected var taskAttemptId: TaskAttemptID = _
  protected var outgoingMessageManager: OutgoingMessageManager[M] = _
  protected var localQueue: MessageQueue[M] = _
  protected var localQueueForNextIteration: SynchronizedQueue[M] = _
  protected var hermes: Hermes = _

  override def initialize(sys: ActorSystem) {
    if(null == sys)
      throw new IllegalArgumentException("ActorSystem is missin!")
    this.hermes = TypedActor(sys).typedActorOf(TypedProps[Iris]())
    if(null == this.hermes)
      throw new RuntimeException("Fail initializing bridge on behalf of "+
                                 "DefaultMessageManager sends messages.")
    if(null == this.configuration)
      throw new RuntimeException("Common configuration is not yet set!")
    this.hermes.initialize(configuration)
  }

  /**
   * Initialize messaging service with common configuration provided by 
   * {@link BSPPeerContainer}.
   * @param conf is common configuration, not task configuration.
   * @param taskAttemptId is specific task attempt id to be used during 
   *                      messenging.
   */
  override def init(conf: HamaConfiguration, taskAttemptId: TaskAttemptID) {
    this.taskAttemptId = taskAttemptId
    if(null == this.taskAttemptId)
      throw new IllegalArgumentException("TaskAttemptID is missing!")
    this.configuration = conf
    if(null == this.configuration)
      throw new IllegalArgumentException("HamaConfiguration is missing!")

    this.localQueue = getReceiverQueue
    this.localQueueForNextIteration = getSynchronizedReceiverQueue
    this.outgoingMessageManager = OutgoingMessageManager.get[M](configuration)
  }

  /**
   * Memory queue doesn't perform any initialization after initialize() gets 
   * called.
   * @return MessageQueue type is backed with a particular queue implementation.
   */
  def getReceiverQueue: MessageQueue[M] = { 
    val queue = MessageQueue.get[M](configuration)
    queue.init(configuration, taskAttemptId)
    queue
  }

  def getSynchronizedReceiverQueue: SynchronizedQueue[M] = 
    SingleLockQueue.synchronize(getReceiverQueue)

  override def close() {
    outgoingMessageManager.clear
    localQueue.close
    cleanupDiskQueue
  }

  def cleanupDiskQueue() {
    try {
      val operation = Operation.get(this.configuration)
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

  /**
   * When the client calls this function, following actions are taken place: 
   * - lookup remote ActorRef 
   * - once received reply, adding ActorRef to LRUCache.
   */
  @throws(classOf[IOException])
  override def send(peerName: String, msg: M) = {
    outgoingMessageManager.addMessage(PeerInfo.fromString(peerName), msg); 
    // TODO: increment counter by 1
    // peer.incrementCounter(BSPPeerImpl.PeerCounter.TOTAL_MESSAGES_SENT, 1L)
  }

  override def getOutgoingBundles(): 
    java.util.Iterator[java.util.Map.Entry[PeerInfo, BSPMessageBundle[M]]] = 
    outgoingMessageManager.getBundleIterator

  /**
   * Actual transfer messsages over wire.
   * It first finds the peer and then send messages.
   * @param peer contains information of another peer. 
   * @param bundle are messages to be sent.
   */
  @throws(classOf[IOException])
  override def transfer(peer: PeerInfo, bundle: BSPMessageBundle[M]) = 
    this.hermes.transfer(peer, bundle) 

  @throws(classOf[IOException])
  override def loopBackMessages(bundle: BSPMessageBundle[M]) {}

  @throws(classOf[IOException])
  override def loopBackMessage(message: Writable) {} 

  override def getListenerAddress(): PeerInfo = null

}
