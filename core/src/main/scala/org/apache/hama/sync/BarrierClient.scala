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
package org.apache.hama.sync

import akka.actor.ActorRef
import java.net.InetAddress
import org.apache.hama.bsp.BSPJobID
import org.apache.hama.bsp.TaskAttemptID
import org.apache.hama.bsp.v2.Task
import org.apache.hama.LocalService
import org.apache.hama.HamaConfiguration
import org.apache.hama.Close
import org.apache.hama.logging.Logging
import org.apache.hama.logging.LoggingAdapter
import org.apache.hama.logging.TaskLog
import org.apache.hama.logging.TaskLogger
import org.apache.hama.logging.TaskLogging

sealed trait BarrierMessage
final case class SetTaskAttemptId(taskAttemptId: TaskAttemptID) 
      extends BarrierMessage
final case class GetPeerNameBy(index: Int) extends BarrierMessage
final case object GetPeerName extends BarrierMessage
final case object GetNumPeers extends BarrierMessage
final case object GetAllPeerNames extends BarrierMessage
final case class Enter(superstep: Long) extends BarrierMessage
final case object WithinBarrier extends BarrierMessage
final case class Leave(superstep: Long) extends BarrierMessage
final case object ExitBarrier extends BarrierMessage

object BarrierClient {

  /**
   * Create Java based barrier client object.
   * @param conf is common configuration.
   * @param taskAttemptId denotes on which task this client operate. 
   * @param host denotes the machine on which the process runs.
   * @param port denotes the port used by the process.
   */
  def get(conf: HamaConfiguration, taskAttemptId: TaskAttemptID): 
      PeerSyncClient = {
    val syncClient = SyncServiceFactory.getPeerSyncClient(conf)
    syncClient.init(conf, taskAttemptId.getJobID, taskAttemptId)
    val host = conf.get("bsp.peer.hostname", 
                        InetAddress.getLocalHost.getHostName)
    val port = conf.getInt("bsp.peer.port", 61000)
    syncClient.register(taskAttemptId.getJobID, taskAttemptId, host, port)
    syncClient
  }

}

// TODO: switch to curator distributed double barrier or add retry func.
class BarrierClient(conf: HamaConfiguration, // common conf
                    syncClient: PeerSyncClient,
                    tasklog: ActorRef) 
      extends LocalService with TaskLog { 

  protected var taskAttemptId: Option[TaskAttemptID] = None

  override def LOG: LoggingAdapter = Logging[TaskLogger](tasklog)

  override def configuration(): HamaConfiguration = conf

  protected def setTaskAttemptId: Receive = {
    case SetTaskAttemptId(taskAttemptId) =>
      this.taskAttemptId = Option(taskAttemptId)
  }

  protected def currentPeerName: Receive = {
    case GetPeerName => sender ! syncClient.getPeerName
  }

  protected def peerNameByIndex: Receive = {
    case GetPeerNameBy(index) => taskAttemptId.map { (id) => 
      initPeers(id) match {
        case null => LOG.error("Unlikely but the peers array found is null!")
        case allPeers@_ => allPeers.isEmpty match {
          case true => LOG.error("Empty peers with task {}! ", id)
          case false => {
            val peer = allPeers(index)
            LOG.debug("Request index {} has value {}. All peers vlaue is {} ", 
                      index, peer, allPeers.mkString(", "))
            sender ! peer
          }
        }
      }
    }
  }

  protected def numPeers: Receive = {
    case GetNumPeers => taskAttemptId.map { (id) => initPeers(id) match {
      case null => sender ! 0
      case allPeers@_ => sender ! allPeers.length
    }}
  }
  
  protected def allPeerNames: Receive = {
    case GetAllPeerNames => taskAttemptId.map { (id) => sender ! initPeers(id) }
  }

  protected def initPeers(taskAttemptId: TaskAttemptID): Array[String] = 
    syncClient.getAllPeerNames(taskAttemptId)

  protected def enter: Receive = {
    case Enter(superstep) => taskAttemptId.map { (id) => {
      LOG.debug("Enter barrier with task attempt id {}", id)
      syncClient.enterBarrier(id.getJobID, id, superstep)
      withinBarrier(sender)
    }}
  }

  protected def withinBarrier(from: ActorRef) = from ! WithinBarrier

  protected def leave: Receive = {
    case Leave(superstep) => taskAttemptId.map { (id) => {
      syncClient.leaveBarrier(id.getJobID, id, superstep)
      LOG.debug("Leave barrier with task attempt id {}", id)
      exitBarrier(sender)
    }}
  }

  protected def exitBarrier(from: ActorRef) = from ! ExitBarrier

  protected def close: Receive = {
    case Close => {
      // perform some close operations and then stop the actor
      context.stop(self) 
    }
  }

  override def receive = setTaskAttemptId orElse currentPeerName orElse peerNameByIndex orElse numPeers orElse allPeerNames orElse enter orElse leave orElse close orElse unknown

}
