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
import java.io.IOException
import java.net.InetAddress
import java.net.URLClassLoader
import java.util.Iterator
import java.util.Map.Entry
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Writable
import org.apache.hama.Close
import org.apache.hama.HamaConfiguration
import org.apache.hama.LocalService
import org.apache.hama.ProxyInfo
import org.apache.hama.bsp.BSPJobID
import org.apache.hama.bsp.Counters
import org.apache.hama.bsp.TaskAttemptID
import org.apache.hama.fs.CacheService
import org.apache.hama.fs.Operation
import org.apache.hama.logging.Logging
import org.apache.hama.logging.LoggingAdapter
import org.apache.hama.logging.TaskLog
import org.apache.hama.logging.TaskLogging
import org.apache.hama.logging.TaskLogger
import org.apache.hama.message.BSPMessageBundle
import org.apache.hama.message.ClearOutgoingMessages
import org.apache.hama.message.GetCurrentMessage
import org.apache.hama.message.GetNumCurrentMessages
import org.apache.hama.message.GetOutgoingBundles
import org.apache.hama.message.Send
import org.apache.hama.message.Transfer
import org.apache.hama.message.TransferredCompleted
import org.apache.hama.message.TransferredFailure
import org.apache.hama.message.TransferredState
//import org.apache.hama.message.MessageManager
//import org.apache.hama.message.Messenger
//import org.apache.hama.monitor.CheckpointerReceiver
//import org.apache.hama.monitor.Checkpointable
//import org.apache.hama.sync.BarrierClient
import org.apache.hama.sync.BarrierMessage
import org.apache.hama.sync.Enter
import org.apache.hama.sync.WithinBarrier
import org.apache.hama.sync.GetNumPeers
import org.apache.hama.sync.GetPeerName
import org.apache.hama.sync.GetPeerNameBy
import org.apache.hama.sync.Leave
import org.apache.hama.sync.OutsideBarrier
import org.apache.hama.sync.PeerSyncClient
import org.apache.hama.sync.SyncException
import org.apache.hama.util.Utils
import scala.collection.JavaConversions._
import scala.util.Try
import scala.util.Success
import scala.util.Failure

final case class Setup(task: Task)

/**
 * This class provides BSPPeer functions in terms of Actor, and will be 
 * encapsulated in an adapter used by {@link Superstep}s.
 *
 * {@link Coordinator} is responsible for providing related services, 
 * including:
 * - messenging
 * - <strik>io</strike>
 * - sync
 */
// TODO: use task operator to collect metrics: 
//  - status
//  - start time
//  - finish time
//  - progress, etc.
// TODO: use wrapper to wrap BSPPeer which calls Coordinator instead!
class Coordinator(conf: HamaConfiguration,  // common conf
                  controller: ActorRef,
                  checkpointer: ActorRef,
                  messenger: ActorRef, 
                  syncClient: ActorRef,
                  tasklog: ActorRef) extends LocalService with TaskLog {
  type ProxyAndBundleIt = Iterator[Entry[ProxyInfo, BSPMessageBundle[Writable]]]
  /**
   * Create a new task when passing task to the next actor.
   */
  var task: Option[Task] = None

  override def LOG: LoggingAdapter = Logging[TaskLogger](tasklog)

  override def configuration(): HamaConfiguration = conf

  //override def initializeServices = localize(conf)

  protected def setup: Receive = {
    case Setup(task: Task) => {
      this.task = Option(task)
      setup(task)  
    }
  } 

  /**
   * Copy necessary files to local (file) system so to speed up computation.
   * @param conf should contain related cache files if any.
   */
  protected def localize(conf: HamaConfiguration) = 
    CacheService.moveCacheToLocal(conf)

  /**
   * Prepare related data for a specific task.
   * Note: <pre>configuration != task.getConfiguration(). </pre>
   *       The formor comes from child process, the later from the task.
   * @param task contains setting for a specific job; task configuration differs
   *             from conf provided by {@link Container}.
   */
  protected[v2] def setup(task: Task) {  
    settingFor(task)  
    localize(conf)
    firstSync(task)  
  }
  
  /**
   * - Configure FileSystem's working directory with corresponded 
   * <b>task.getConfiguration()</b>.
   * - And add additional classpath to task's configuration.
   * @param conf is the common setting from bsp peer container.
   * @param task contains setting for particular job computation.
   */
  protected def settingFor(task: Task) = {
    val taskConf = task.getConfiguration

    //TODO: this seems to belong to io
    //Operation.get(taskConf).setWorkingDirectory(
      //new Path(Operation.defaultWorkingDirectory(taskConf))
    //) 

    val libjars = CacheService.moveJarsAndGetClasspath(conf) 
    libjars match {
      case null => LOG.warning("No jars to be included for "+task.getId)
      case _ => {
        LOG.info("Jars to be included in classpath are "+
                 libjars.mkString(", "))
        taskConf.setClassLoader(new URLClassLoader(libjars, 
                                                   taskConf.getClassLoader))
      }
    } 
  }

  /**
   * Internal sync to ensure all peers is registered/ ready.
   * @param superstep indicate the curent superstep value.
   */
  //TODO: should the task's superstep be confiured to 0 instead?
  protected def firstSync(task: Task) {
    enterBarrier(task.getId, task.getCurrentSuperstep)
    leaveBarrier(task.getId, task.getCurrentSuperstep)
    task.increatmentSuperstep
  }

  @throws(classOf[IOException])
  def send(peerName: String, msg: Writable) = 
    messenger ! Send(peerName, msg.asInstanceOf[Writable])

  protected def getCurrentMessage(): Writable = 
    Utils.await[Writable](messenger, GetCurrentMessage)

  protected def getNumCurrentMessages(): Int = 
    Utils.await[Int](messenger, GetNumCurrentMessages)

  protected def getSuperstepCount(): Long = task.map { (aTask) =>
    aTask.getCurrentSuperstep 
  }.getOrElse(0).asInstanceOf[Long]

  protected def getPeerName(): String = 
    Utils.await[String](syncClient, GetPeerName)

  protected def getPeerName(index: Int): String = task.map { (aTask) =>
    Utils.await[String](syncClient, GetPeerNameBy(aTask.getId, index))
  }.getOrElse(null)

  protected def getNumPeers(): Int = Utils.await[Int](syncClient, GetNumPeers)

  protected def getPeerIndex(): Int = task.map { (task) => 
    task.getId.getTaskID.getId 
  }.getOrElse(0)

  protected def getTaskAttemptId(): TaskAttemptID = task.map { (task) => 
    task.getId 
  }.getOrElse(null) 

  @throws(classOf[IOException])
  protected def sync() {
    task.map { (aTask) => {
      aTask.transitToSync 
       
      enterBarrier(aTask.getId, aTask.getCurrentSuperstep)

      val it = Utils.await[ProxyAndBundleIt](messenger, GetOutgoingBundles)
      asScalaIterator(it).foreach( entry => {
        val peer = entry.getKey
        val bundle = entry.getValue
        it.remove 
        messenger ! Transfer(peer, bundle)
      })

      Utils.await[TransferredState](messenger, "IsTransferredCompleted") match {
        case TransferredCompleted => {
          LOG.debug("Message transfers completely for task {}.", aTask.getId)
          clear
        }
        case TransferredFailure => {
          LOG.error("Fail transferring messages for task {}!", aTask.getId)
          // ask controller to recover from the latest superstep
          controller ! TransferredFailure 
        }   
      }       
      // checkpoint(messenger, nextPack(conf)) TODO: 
      // TODO: record time elapsed between enterBarrier and leaveBarrier, etc.
      leaveBarrier(aTask.getId, aTask.getCurrentSuperstep)
      aTask.increatmentSuperstep 
    }}
  } 

  protected def clear() = messenger ! ClearOutgoingMessages 

  @throws(classOf[SyncException])
  protected def enterBarrier(taskAttemptId: TaskAttemptID, superstep: Long) = 
    Utils.await[BarrierMessage](syncClient, Enter(taskAttemptId, superstep))

  @throws(classOf[SyncException])
  protected def leaveBarrier(taskAttemptId: TaskAttemptID, superstep: Long) = 
    Utils.await[BarrierMessage](syncClient, Leave(taskAttemptId, superstep))

  /**
   * This is called after {@link BSP#bsp} finishs its execution in the end.
   * It will close all necessary operations.
   */
   // TODO: close all operations, including message/ sync/ local files in cache, etc.
  protected[v2] def close() = {
    clear 
    syncClient ! Close 
    messenger ! Close
  }   

  override def receive = setup orElse unknown
}
