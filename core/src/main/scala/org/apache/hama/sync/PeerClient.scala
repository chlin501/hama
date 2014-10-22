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
import org.apache.hama.Agent
import org.apache.hama.HamaConfiguration
import org.apache.hama.Close
import org.apache.hama.logging.Logging
import org.apache.hama.logging.LoggingAdapter
import org.apache.hama.logging.TaskLog
import org.apache.hama.logging.TaskLogger
import org.apache.hama.logging.TaskLogging

sealed trait PeerClientMessage
final case object Register extends PeerClientMessage
final case class GetPeerNameBy(index: Int) extends PeerClientMessage
final case class PeerNameByIndex(name: String) extends PeerClientMessage
final case object GetPeerName extends PeerClientMessage
final case class PeerName(peerName: String) extends PeerClientMessage
final case object GetNumPeers extends PeerClientMessage
final case class NumPeers(num: Int) extends PeerClientMessage
final case object GetAllPeerNames extends PeerClientMessage
final case class AllPeerNames(allPeers: Array[String]) extends PeerClientMessage
final case class Enter(superstep: Long) extends PeerClientMessage
final case object WithinBarrier extends PeerClientMessage
final case class Leave(superstep: Long) extends PeerClientMessage
final case object ExitBarrier extends PeerClientMessage

/**
 * An wrapper that help deal with barrier synchronization and post/ retrieve
 * peer name, etc. information.
 * @param conf denotes comon conf that contains actor system, etc. information.
 * @param taskAttemptId denotes to which id this peer is bound.
 * @param syncer provides barrier synchronization functions.
 * @param operator deals with peer data registration and retrievation. 
 * @param tasklog logs task info.
 */
class PeerClient(conf: HamaConfiguration, 
                 taskAttemptId: TaskAttemptID,
                 syncer: Barrier,
                 operator: PeerRegistrator, 
                 tasklog: ActorRef) extends Agent with TaskLog { 
 
  protected var allPeers: Option[Array[String]] = None

  protected def register: Receive = {
    case Register => {
      val seq = conf.getInt("bsp.child.slot.seq", -1)  
      if(-1 == seq) throw new RuntimeException("Peer's slot seq `-1' is not "+
                                               "correctly configured!")
      val sys = conf.get("bsp.child.actor-system.name", "BSPPeerSystem"+seq)
      val host = conf.get("bsp.peer.hostname", 
                          InetAddress.getLocalHost.getHostName)
      val port = conf.getInt("bsp.peer.port", 61000)
      LOG.debug("ActorSystem {}, host {}, port {} is going to be registered!", 
                sys, host, port)  
      operator.register(taskAttemptId, sys, host, port)
    }
  }

  protected def currentPeerName: Receive = {
    case GetPeerName => sender ! PeerName(operator.getPeerName) 
  }

  protected def peerNameByIndex: Receive = {
    case GetPeerNameBy(index) => initPeers(taskAttemptId) match {
      case null => {
        LOG.error("Unlikely but the peers array found is null for task {}!",
                  taskAttemptId)
        sender ! PeerNameByIndex(null) 
      }
      case peers@_ => peers.isEmpty match {
        case true => {
          LOG.error("Empty peers for task {}! ", taskAttemptId)
          sender ! PeerNameByIndex("") 
        }
        case false => {
          val peer = peers(index)
          LOG.debug("Request index {} has value {}. All peers vlaue is {} ", 
                    index, peer, peers.mkString(", "))
          sender ! PeerNameByIndex(peer)
        }
      }
    }
  }

  protected def numPeers: Receive = {
    case GetNumPeers => initPeers(taskAttemptId) match {
      case null => sender ! NumPeers(0) // TODO: reply failure message?
      case peers@_ => sender ! NumPeers(peers.length)
    }
  }
  
  protected def allPeerNames: Receive = {
    case GetAllPeerNames => sender ! AllPeerNames(initPeers(taskAttemptId))
  }

  protected def initPeers(taskAttemptId: TaskAttemptID): Array[String] = 
    allPeers match { 
      case None => operator.getAllPeerNames(taskAttemptId).sorted
      case Some(array) => array.sorted
    }

  protected def enter: Receive = {
    case Enter(superstep) => {
      //LOG.debug("Enter barrier at superstep {} for task attempt id {}", 
       //         superstep, taskAttemptId)
      val start = System.currentTimeMillis // TODO: move to util
      syncer.enter(superstep) 
      val elapsed = System.currentTimeMillis - start
      //LOG.debug("After java enter barrier func, time spent: {} for task {}", 
                //(elapsed/1000d), taskAttemptId)
      withinBarrier(sender)
      //LOG.debug("WithinBarrier msg sent to coordinator for task {}", 
                //taskAttemptId)
    }
  }

  protected def withinBarrier(from: ActorRef) = from ! WithinBarrier

  protected def leave: Receive = {
    case Leave(superstep) => {
      //LOG.debug("Leave barrier at superstep {} for task attempt id {}", 
                //superstep, taskAttemptId)
      val start = System.currentTimeMillis // TODO: move to util
      syncer.leave(superstep)
      val elapsed = System.currentTimeMillis - start
      //LOG.debug("After java leave barrier func, time spent: {} for task {}", 
               //(elapsed/1000d), taskAttemptId)
      exitBarrier(sender)
      //LOG.debug("ExitBarrier msg sent to coordinator for task {}", 
                //taskAttemptId)
    }
  }

  protected def exitBarrier(from: ActorRef) = from ! ExitBarrier

  /**
   * Perform some close operations and then stop the actor.
   */
  protected def close: Receive = {
    case Close => {
      context.stop(self) 
    }
  }

  override def receive = register orElse currentPeerName orElse peerNameByIndex orElse numPeers orElse allPeerNames orElse enter orElse leave orElse close orElse unknown

}
