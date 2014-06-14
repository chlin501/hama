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
import org.apache.hama.HamaConfiguration
import org.apache.hama.bsp.BSPMessageBundle
import org.apache.hama.message.queue.MessageQueue
import org.apache.hama.message.queue.MemoryQueue

/**
 * Provide default functionality of {@link MessageManager}.
 */
class DefaultMessageManager[M <: Writable] extends MessageManager[M]
                                           with Actor /*with Curator*/ {

  val LOG = Logging(context.system, this)
 
  private var configuration: HamaConfiguration = _
  protected var localQueue: MessageQueue[M] = _

  override def initialize(conf: HamaConfiguration) = {
    this.configuration = configuration
    this.localQueue = getReceiverQueue
    //initializeCurator(configuration)
    // TODO: initialize with task attempt id? 
    //       create znode path /bsp/superstep/<job_id>/<superstep>/<task_attemptId> for communication?
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
    queue.initialize(configuration) 
    queue
  }


  override def close() {}

  @throws(classOf[IOException])
  override def currentMessage(): M = null.asInstanceOf[M]

  @throws(classOf[IOException])
  override def send(peerName: String, msg: M) = null.asInstanceOf[M]

  override def outgoingBundles(): 
      Iter[Entry[InetSocketAddress, BSPMessageBundle[M]]] = 
    null.asInstanceOf[Iter[Entry[InetSocketAddress, BSPMessageBundle[M]]]] 

  @throws(classOf[IOException])
  override def transfer(addr: InetSocketAddress, bundle: BSPMessageBundle[M]) {
  }

  override def clearOutgoingMessages() {}

  override def numCurrentMessages(): Int = -1

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
    case Initialize(conf) => initialize(conf)
  }

  override def receive = setup orElse unknown

}
