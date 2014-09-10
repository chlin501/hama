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
import org.apache.hama.bsp.message.queue.DiskQueue // TODO: move to hama.message.queue instead
import org.apache.hama.bsp.TaskAttemptID
import org.apache.hama.fs.Operation
import org.apache.hama.HamaConfiguration
import org.apache.hama.message.compress.BSPMessageCompressor
import org.apache.hama.message.queue.MemoryQueue
import org.apache.hama.message.queue.MessageQueue
import org.apache.hama.message.queue.SingleLockQueue
import org.apache.hama.message.queue.SynchronizedQueue
import org.apache.hama.message.queue.Viewable
import org.apache.hama.ProxyInfo
import org.apache.hama.util.LRUCache
import org.apache.hama.logging.CommonLog
import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.util.{Try, Failure, Success}

/**
 * A bridge device enables bsp peers communication through TypedActor.
 */
trait PeerCommunicator {
  
  /**
   * Bind {@link PeerMessenger} with its underlying implementation.
   * @param peerMessenger
   */
  def communicator(peerMessenger: ActorRef)

}

/**
 * Provide default functionality of {@link MessageManager}.
 * It realizes message communication by java object, and send messages through
 * actor via {@link akka.actor.TypedActor}.
 * @param conf is the common configuration, not specific for task.
 */
class DefaultMessageManager[M <: Writable] extends MessageManager[M] 
                                           with PeerCommunicator
                                           with MessageView
                                           with CommonLog {

  // TODO: change to Option[...]
  protected var configuration: HamaConfiguration = _
  protected var taskAttemptId: TaskAttemptID = _
  protected var outgoingMessageManager: OutgoingMessageManager[M] = _
  protected var localQueue: MessageQueue[M] = _ // Option[MessageQueue[M]]
  protected var localQueueForNextIteration: SynchronizedQueue[M] = _
  protected var peerMessenger: Option[ActorRef] = None
 
  /**
   * {@link PeerMessenger} address information.
   */
  protected var currentPeer: Option[ProxyInfo] = None

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
        //val bundle = loopbackMessageQueue.take
        val bundle = PeerMessenger.loopbackQueue.take
        if(null == bundle) 
          throw new RuntimeException("Bundle received is null!")
        receiver.loopBackMessages(bundle.asInstanceOf[BSPMessageBundle[M]]) 
      }
      true
    }
  }

  override def communicator(mgr: ActorRef) = peerMessenger = Option(mgr)

  /**
   * Indicate the local peer.
   */
  protected def currentPeerInfo(conf: HamaConfiguration): 
      Either[RuntimeException, ProxyInfo] = {
    val seq = conf.getInt("bsp.child.slot.seq", -1)
    seq match {
      case -1 => Left(new RuntimeException("Slot seq -1 is not configured!")) 
      case _ => {
        val host = conf.get("bsp.peer.hostname", 
                            InetAddress.getLocalHost.getHostName) 
        val port = conf.getInt("bsp.peer.port", 61000)
        val addr = "BSPPeerSystem%d@%s:%d".format(seq, host, port)
        LOG.info("Current peer address is "+addr)
        Right(Peer.at(addr))
      }
    }
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
    currentPeerInfo(configuration) match {
      case Left(e) => throw e
      case Right(info) => this.currentPeer = Some(info)
    }
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
  protected def getReceiverQueue: MessageQueue[M] = { 
    val queue = MessageQueue.get[M](configuration)
    queue.init(configuration, taskAttemptId)
    queue
  }

  protected def getSynchronizedReceiverQueue: SynchronizedQueue[M] = 
    SingleLockQueue.synchronize(getReceiverQueue)

  override def close() {
    executor.shutdown
    outgoingMessageManager match {
      case null =>
      case _ => outgoingMessageManager.clear
    }
    localQueue match {
      case null =>
      case _ => localQueue.close
    }
    cleanupDiskQueue match {
      case Success(yOrN) => LOG.debug("Need to cleanup disk queue? "+yOrN)
      case Failure(ioe) => LOG.warning("Fail cleaning up disk queue for "+
                                       taskAttemptId+"!",ioe)
    }
  }

  /**
   * Cleanup disk queue if needed. 
   * @return Boolean denotes that the disk is cleanup when true; otherwise 
   *                 false.
   */ 
  def needToCleanup(taskAttemptId: TaskAttemptID): Boolean = {
    taskAttemptId match {
      case null => false
      case _ => {
        val operation = Operation.get(this.configuration)
        val diskQueueDir = configuration.get("bsp.disk.queue.dir")
        operation.remove(DiskQueue.getQueueDir(configuration, 
                                             taskAttemptId,
                                             diskQueueDir))
        true
      }
    }
  }

  protected def cleanupDiskQueue(): Try[Boolean] = 
    Try(needToCleanup(taskAttemptId))

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
  override def clearOutgoingMessages() { // TODO: use f style not if-else
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
  @throws(classOf[IOException])
  override def send(peerName: String, msg: M) {
    outgoingMessageManager.addMessage(Peer.at(peerName), msg); 
    // TODO: increment counter by 1 / aggregate to stats api.
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
  override def transfer(peer: ProxyInfo, bundle: BSPMessageBundle[M]) = 
    peerMessenger match {
      case Some(found) =>  found ! Transfer(peer, bundle)
      case None =>
    }

  @throws(classOf[IOException])
  override def loopBackMessages(bundle: BSPMessageBundle[M]) = 
      this.synchronized {
    val threshold =
      configuration.getLong("hama.messenger.compression.threshold", 128) 
    bundle.setCompressor(BSPMessageCompressor.get(configuration), threshold)
    val it: java.util.Iterator[_ <: Writable] = bundle.iterator
    while (it.hasNext) loopBackMessage(it.next)
  }

  @throws(classOf[IOException])
  override def loopBackMessage(message: Writable) {
    localQueueForNextIteration.add(message.asInstanceOf[M])
    //TODO: stats peer.incrementCounter(BSPPeerImpl.PeerCounter.TOTAL_MESSAGES_RECEIVED, 1L);
  } 

  override def getListenerAddress(): ProxyInfo = this.currentPeer match {
    case None => 
      throw new RuntimeException("Peer is not initialized because it's null!")
    case Some(info) => info
  }

}
