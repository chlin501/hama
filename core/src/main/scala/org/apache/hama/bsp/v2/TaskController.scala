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
package org.apache.hama.bsp.v2

import akka.actor.ActorRef
import java.util.Iterator
import java.util.Map.Entry
import org.apache.hadoop.io.Writable
import org.apache.hama.Close
import org.apache.hama.HamaConfiguration
import org.apache.hama.LocalService
import org.apache.hama.ProxyInfo
import org.apache.hama.message.BSPMessageBundle
import org.apache.hama.message.ClearOutgoingMessages
import org.apache.hama.message.GetCurrentMessage
import org.apache.hama.message.GetNumCurrentMessages
import org.apache.hama.message.GetOutgoingBundles
import org.apache.hama.message.IsTransferredCompleted
import org.apache.hama.message.Transfer
import org.apache.hama.message.TransferredCompleted
import org.apache.hama.message.TransferredFailure
import org.apache.hama.message.TransferredState
import org.apache.hama.sync.BarrierMessage
import org.apache.hama.sync.Enter
import org.apache.hama.sync.ExitBarrier
import org.apache.hama.sync.Leave
import org.apache.hama.sync.PeerSyncClient
import org.apache.hama.sync.SyncException
import org.apache.hama.sync.WithinBarrier
import scala.collection.JavaConversions._

sealed trait TaskStatMessage
final case object GetSuperstepCount extends TaskStatMessage
final case object GetPeerIndex extends TaskStatMessage
final case object GetTaskAttemptId extends TaskStatMessage

class TaskController(conf: HamaConfiguration, // common conf
                     messenger: ActorRef,
                     syncClient: ActorRef) 
      extends LocalService /*with TaskLog*/ {

  type ProxyAndBundleIt = Iterator[Entry[ProxyInfo, BSPMessageBundle[Writable]]]

  protected var task: Option[Task] = None

  override def configuration(): HamaConfiguration = conf

  /**
   * - Update task phase to sync // perhaps more detail phase value e.g. within barrier
   * - Enter barrier sync 
   */
  protected def enter: Receive = {
    case Enter(superstep) => {
      task.map { (aTask) => transitToSync(aTask) }
      syncClient ! Enter(superstep)
    }
  }

  // TODO: further divide task sync phase
  protected def transitToSync(task: Task) = task.transitToSync 

  /**
   * {@link PeerSyncClient} reply passing enter function.
   */
  protected def inBarrier: Receive = {
    case WithinBarrier => task.map { (aTask) => withinBarrier(aTask) }
  }

  protected def withinBarrier(task: Task) = getBundles()

  /**
   * Obtain message bundles sent through {@link BSPPeer#send} function.
   */
  protected def getBundles() = messenger ! GetOutgoingBundles

  /**
   * Transmit message bundles iterator to remote messenger.
   */
  protected def proxyBundleIterator: Receive = {
    case it: ProxyAndBundleIt => task.map { (aTask) => {
      transmit(it, aTask) 
      checkIfTransferredCompleted(aTask)
    }}
  }

  protected def checkIfTransferredCompleted(task: Task) = 
    messenger ! IsTransferredCompleted 

  protected def transmit(it: ProxyAndBundleIt, task: Task) = 
    asScalaIterator(it).foreach( entry => {
      val peer = entry.getKey
      val bundle = entry.getValue
      it.remove 
      messenger ! Transfer(peer, bundle)
    })

  /**
   * Clear messenger's outgoing bundle queue.
   * Leave barrier sync. 
   */
  protected def transferredCompleted: Receive = {
    case TransferredCompleted => task.map { (aTask) => 
      clear
      self ! Leave(aTask.getCurrentSuperstep)
    }
  }

  protected def transferredFailure: Receive = {
    case TransferredFailure =>  // TODO: report container, which in turns calls to master, do restart worker on other node!
  }
 
  protected def leave: Receive = {
    case Leave(superstep) => syncClient ! Leave(superstep)
  }

  protected def exitBarrier: Receive = {
    case ExitBarrier => // TODO: call to next superstep
  }

  /**
   * This is called after {@link BSP#bsp} finishs its execution in the end.
   * It will close all necessary operations.
   */
   // TODO: close all operations, including message/ sync/ local files in 
   //       cache, etc.
   //       collect combiner stats:  
   //         total msgs combined = total msgs sent - total msgs received
  protected[v2] def close() = {
    clear 
    syncClient ! Close 
    messenger ! Close
  }

  /**
   * BSPPeer ask controller at which superstep count this task now is.
   */
  protected def superstepCount: Receive = {
    case GetSuperstepCount => task.map { (aTask) => 
      sender ! aTask.getCurrentSuperstep
    }
  }

  /**
   * BSPPeer ask controller the index of this peer.
   */
  protected def peerIndex: Receive = {
    case GetPeerIndex => task.map { (aTask) => 
      sender ! aTask.getId.getTaskID.getId 
    }
  }

  /**
   * BSPPeer ask controller which task attempt id it is.
   */
  protected def taskAttemptId: Receive = {
    case GetTaskAttemptId => task.map { (aTask) => sender ! aTask.getId }
  }

  protected def clear() = messenger ! ClearOutgoingMessages

  override def receive = enter orElse inBarrier orElse proxyBundleIterator orElse transferredCompleted orElse transferredFailure orElse leave orElse exitBarrier orElse superstepCount orElse peerIndex orElse taskAttemptId orElse unknown 

}
