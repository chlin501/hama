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
import akka.event.Logging

import java.io.IOException
import java.net.BindException
import java.net.InetSocketAddress
import java.util.ArrayList
import java.util.{ Iterator => Iter }
import java.util.Map.Entry

import org.apache.hadoop.io.Writable
import org.apache.hadoop.util.ReflectionUtils
import org.apache.hama.bsp.TaskAttemptID
import org.apache.hama.HamaConfiguration
import org.apache.hama.message.compress.BSPMessageCompressor
import org.apache.hama.message.compress.BSPMessageCompressorFactory
import org.apache.hama.message.queue.MemoryQueue
import org.apache.hama.message.queue.MessageQueue
import org.apache.hama.message.queue.SingleLockQueue
import org.apache.hama.message.queue.SynchronizedQueue

/**
 * Provide default functionality of {@link MessageManager}.
 */
class DefaultMessageManager[M <: Writable] extends MessageManager[M]
                                           with Actor /*with Curator*/ {

  val LOG = Logging(context.system, this)
 
  private var configuration: HamaConfiguration = _
  protected var taskAttemptId: TaskAttemptID = _
  protected var compressor: BSPMessageCompressor[M] = _
  protected var outgoingMessageManager: OutgoingMessageManager[M] = _
  protected var localQueue: MessageQueue[M] = _
  protected var localQueueForNextIteration: SynchronizedQueue[M] = _

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
    // delete disk queue based on task attempt id
  }

  @throws(classOf[IOException])
  override def currentMessage(): M = localQueue.poll
  

  @throws(classOf[IOException])
  override def send(peerName: String, msg: M) = null.asInstanceOf[M]

  override def outgoingBundles(): 
      Iter[Entry[InetSocketAddress, BSPMessageBundle[M]]] = 
    outgoingMessageManager.getBundleIterator

  @throws(classOf[IOException])
  override def transfer(addr: InetSocketAddress, bundle: BSPMessageBundle[M]) {
  }

  override def clearOutgoingMessages() {}

  override def numCurrentMessages(): Int = localQueue.size

  @throws(classOf[IOException])
  override def loopBackMessages(bundle: BSPMessageBundle[M]) {}

  @throws(classOf[IOException])
  override def loopBackMessage(message: Writable) {} 

  override def listenerAddress(): InetSocketAddress = null

  def unknown: Receive = {
    case msg@ _ => LOG.warning("Unknown message {} received by {}", 
                               getClass.getName, msg);
  }

  /**
   * Initialize necessary steps for exchaning messages between {@link BSPPeer}.
   * @return Receive is partial function.
   */
  def setup: Receive = {
    case Initialize(conf, taskAttemptId) => {
      throw new IllegalArgumentException("HamaConfiguration is missing!")
      init(conf, taskAttemptId)
    }
  }

  override def receive = setup orElse unknown

}
