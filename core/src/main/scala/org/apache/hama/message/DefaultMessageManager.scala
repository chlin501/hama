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
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.ExecutorService
import org.apache.hadoop.conf.Configurable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Writable
import org.apache.hama.bsp.message.queue.DiskQueue
import org.apache.hama.bsp.TaskAttemptID
import org.apache.hama.fs.Operation
import org.apache.hama.HamaConfiguration
import org.apache.hama.message.compress.BSPMessageCompressor
import org.apache.hama.message.queue.MemoryQueue
import org.apache.hama.message.queue.MessageQueue
import org.apache.hama.message.queue.SingleLockQueue
import org.apache.hama.message.queue.SynchronizedQueue
import org.apache.hama.ProxyInfo
import org.apache.hama.util.LRUCache
import org.apache.hama.logging.Logger
import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.{Failure, Success}


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

  /**
   * An interface calls underlying PeerMessenger.
   */
  protected var hermes: Hermes = _
  
  /**
   * {@link PeerMessenger} address information.
   */
  protected var currentPeer: ProxyInfo = _

  /**
   * This is used for receiving loopback message {@link #loopBackMessages} 
   */ 
  protected val loopbackMessageQueue = 
    new LinkedBlockingQueue[BSPMessageBundle[M]]() 

  /**
   * Only for receiving local messages purpose.
   */
  protected val executor = Executors.newSingleThreadExecutor()

  /**
   * A class that is responsible for receiving BSPMessageBundle for local 
   * use. It will call loppBackMessages once being notified new incoming 
   * messages.
   * @param receiver is the implementation of messenge manager.
   */
  class Loopback[M <: Writable](receiver: MessageManager[M]) 
      extends Callable[Boolean] {
    
    override def call(): Boolean = {
      while(!Thread.currentThread().isInterrupted()) {
        val bundle = loopbackMessageQueue.take
        if(null == bundle) 
          throw new RuntimeException("Fail ispatching local messages received.")
        receiver.loopBackMessages(bundle.asInstanceOf[BSPMessageBundle[M]]) 
      }
      true
    }
  }

  override def initialize(sys: ActorSystem) {
    if(null == sys)
      throw new IllegalArgumentException("ActorSystem is missin!")
    this.hermes = TypedActor(sys).typedActorOf(TypedProps[Iris]())
    if(null == this.hermes)
      throw new RuntimeException("Fail initializing bridge on behalf of "+
                                 "DefaultMessageManager sends messages.")
    if(null == this.configuration)
      throw new RuntimeException("Common configuration is not yet set!")
    this.hermes.initialize[M](configuration, loopbackMessageQueue)
  }

  /**
   * Indicate the local peer.
   */
  def currentPeerInfo(conf: HamaConfiguration): ProxyInfo = {
    val seq = conf.getInt("bsp.child.slot.seq", -1)
    if(-1 == seq)
      throw new RuntimeException("Invalid slot seq "+seq+" for constructing "+
                                 "peer info!")
    val host = conf.get("bsp.peer.hostname", "0.0.0.0")
    val port = conf.getInt("bsp.peer.port", 61000)
    val addr = "BSPPeerSystem%d@%s:%d".format(seq, host, port)
    LOG.info("Current peer address is "+addr)
    Peer.at(addr)
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
    this.currentPeer = currentPeerInfo(configuration)
    this.localQueue = getReceiverQueue
    this.localQueueForNextIteration = getSynchronizedReceiverQueue
    this.outgoingMessageManager = OutgoingMessageManager.get[M](configuration)
    this.executor.submit(new Loopback[M](this))
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
    executor.shutdown
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

  /**
   * This all happens within sync() function.
   * transfer() actually trasmits messages to remote peer or itself, which 
   * in turns calls loopBackMessage(), adding all messages received to 
   * localQueueForNextIteration.
   * After transfer(), bsp peer calls messenger.clearOutgoingMessages. This 
   * function moves all messages from localQueueForNextIteration to localQueue
   * so that bsp peer can obtain messages sent to itself in the next iteration.
   */
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
  override def send(peerName: String, msg: M) {
    outgoingMessageManager.addMessage(Peer.at(peerName), msg); 
    // TODO: increment counter by 1
    // peer.incrementCounter(BSPPeerImpl.PeerCounter.TOTAL_MESSAGES_SENT, 1L)
  }

  override def getOutgoingBundles(): 
    java.util.Iterator[java.util.Map.Entry[ProxyInfo, BSPMessageBundle[M]]] = 
    outgoingMessageManager.getBundleIterator

  /**
   * Actual transfer messsages over wire.
   * It first finds the peer and then send messages.
   * @param peer contains information of another peer. 
   * @param bundle are messages to be sent.
   */
  @throws(classOf[IOException])
  override def transfer(peer: ProxyInfo, bundle: BSPMessageBundle[M]) {
    import ExecutionContext.Implicits.global
    this.hermes.transfer(peer, bundle) onComplete {
      case Failure(failure) => 
        LOG.error("["+failure+"] Fail transferring message to "+peer.getPath)
      case Success(result) => 
        LOG.info("Successful transferring message to "+peer.getPath)
    }
  }

  @throws(classOf[IOException])
  override def loopBackMessages(bundle: BSPMessageBundle[M]) = 
    this.synchronized {
    val threshold =
      configuration.getLong("hama.messenger.compression.threshold", 128) 
    bundle.setCompressor(BSPMessageCompressor.get(configuration), threshold)
    val it: java.util.Iterator[_ <: Writable] = bundle.iterator
    while (it.hasNext) {
      loopBackMessage(it.next)
    }
  }

  @throws(classOf[IOException])
  override def loopBackMessage(message: Writable) {
    localQueueForNextIteration.add(message.asInstanceOf[M])
    //TODO: stats peer.incrementCounter(BSPPeerImpl.PeerCounter.TOTAL_MESSAGES_RECEIVED, 1L);
  } 

  override def getListenerAddress(): ProxyInfo = this.currentPeer 

}
