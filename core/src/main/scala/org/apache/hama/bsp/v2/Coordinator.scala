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
import java.io.File
import java.net.URL
import java.net.URLClassLoader
import java.util.Iterator
import java.util.Map.Entry
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Writable
import org.apache.hama.Clear
import org.apache.hama.Close
import org.apache.hama.HamaConfiguration
import org.apache.hama.LocalService
import org.apache.hama.ProxyInfo
import org.apache.hama.fs.CacheService
import org.apache.hama.fs.Operation
import org.apache.hama.logging.Logging
import org.apache.hama.logging.LoggingAdapter
import org.apache.hama.logging.TaskLog
import org.apache.hama.logging.TaskLogger
import org.apache.hama.logging.TaskLogging
import org.apache.hama.message.BSPMessageBundle
import org.apache.hama.message.ClearOutgoingMessages
import org.apache.hama.message.CurrentMessage
import org.apache.hama.message.GetCurrentMessage
import org.apache.hama.message.GetNumCurrentMessages
import org.apache.hama.message.GetOutgoingBundles
import org.apache.hama.message.NumCurrentMessages
import org.apache.hama.message.IsTransferredCompleted
import org.apache.hama.message.Send
import org.apache.hama.message.Transfer
import org.apache.hama.message.TransferredCompleted
import org.apache.hama.message.TransferredFailure
import org.apache.hama.message.TransferredState
import org.apache.hama.sync.AllPeerNames
import org.apache.hama.sync.BarrierMessage
import org.apache.hama.sync.Enter
import org.apache.hama.sync.ExitBarrier
import org.apache.hama.sync.GetAllPeerNames
import org.apache.hama.sync.GetNumPeers
import org.apache.hama.sync.GetPeerName
import org.apache.hama.sync.GetPeerNameBy
import org.apache.hama.sync.Leave
import org.apache.hama.sync.NumPeers
import org.apache.hama.sync.PeerName
import org.apache.hama.sync.PeerNameByIndex
import org.apache.hama.sync.PeerSyncClient
import org.apache.hama.sync.SetTaskAttemptId
import org.apache.hama.sync.SyncException
import org.apache.hama.sync.WithinBarrier
import org.apache.hama.util.Utils
import scala.collection.JavaConversions._

sealed trait TaskStatMessage
final case object GetSuperstepCount extends TaskStatMessage
final case object GetPeerIndex extends TaskStatMessage
final case object GetTaskAttemptId extends TaskStatMessage

sealed trait CoordinatorMessage
final case class Customize(task: Task) extends CoordinatorMessage
final case object Execute extends CoordinatorMessage

/**
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
// TODO: wrapped by BSPPeerAdapter which calls Coordinator instead!
class Coordinator(conf: HamaConfiguration,  // common conf
                  container: ActorRef, 
                  messenger: ActorRef, 
                  syncClient: ActorRef,
                  tasklog: ActorRef) extends LocalService with TaskLog {

  type ProxyAndBundleIt = Iterator[Entry[ProxyInfo, BSPMessageBundle[Writable]]]

  protected var task: Option[Task] = None

  protected var clients = Map.empty[String, ActorRef]

  override def LOG: LoggingAdapter = Logging[TaskLogger](tasklog)

  override def configuration(): HamaConfiguration = conf

  /**
   * This is the first step:
   * - Config related setting before supersteps are instantiated. 
   */
  protected def customize: Receive = {
    case Customize(task) => customizeFor(task)
  }

  /**
   * Prepare related data for a specific task.
   * Note: <pre>configuration != task.getConfiguration(). </pre>
   *       The formor comes from child process, the later from the task.
   * @param task contains setting for a specific job; task configuration differs
   *             from conf provided by {@link Container}.
   */
  protected def customizeFor(task: Task) {  
    this.task = Option(task)
    localize(conf)
    settingFor(task)  
    configSyncClient(task)
    firstSync(task)  
  }

  protected def execute: Receive = {
    case Execute => doExecute
  }

  protected def doExecute() = task.map { (aTask) => {
    val taskConf = aTask.getConfiguration
    val taskAttemptId = aTask.getId.toString
    addJarToClasspath(taskAttemptId, taskConf)
    // instantiate, cache (ActorRef) and spwan superstep classes e.g. spawn(name, classOf, new BSPPeerAdapter(self))
    // find first superstep in cache 
    // execute the first superstep by sending msg e.g. 1stSueprstep ! Compute (superstep will report back to coordinator with next suprstep class name when compute func finishes, asking for sync, etc.; then coordinator find the next superstep for execution and repeats this process)
  }}

  /**
   * Add jar to classpath so that supersteps can be instantiated.
   * @param taskAttemptId is a task attempt id.
   * @param taskConf is configuration specific to a task.
   */
  protected def addJarToClasspath(taskAttemptId: String, 
                                  taskConf: HamaConfiguration): 
      Option[ClassLoader] = {
    val jar = taskConf.get("bsp.jar")
    LOG.info("Jar path found in task configuration is {}", jar)
    jar match {
      case null|"" => None
      case remoteUrl@_ => {
        val operation = Operation.get(taskConf)
        // TODO: change working directory? see bsppeerimpl or taskworker
        val localJarPath = createLocalPath(taskAttemptId, taskConf, operation) 
        operation.copyToLocal(new Path(remoteUrl))(new Path(localJarPath))
        LOG.info("Remote file {} is copied to {}", remoteUrl, localJarPath) 
        val url = normalizePath(localJarPath)
        val loader = Thread.currentThread.getContextClassLoader
        val newLoader = new URLClassLoader(Array[URL](url), loader) 
        taskConf.setClassLoader(newLoader) 
        LOG.info("User jar {} is added to the newly created url class loader "+
                 "for job {}", url, taskAttemptId)
        Some(newLoader)   
      }
    }
  }

  protected def normalizePath(jarPath: String): URL = 
    new File(jarPath).toURI.toURL // TODO: replace File with neutral api

  protected def createLocalPath(taskAttemptId: String, 
                      config: HamaConfiguration,
                      operation: Operation): String = {
    val localDir = config.get("bsp.local.dir", "/tmp/bsp/local")
    val subDir = config.get("bsp.local.dir.sub_dir", "bspmaster")
    if(!operation.local.exists(new Path(localDir, subDir)))
      operation.local.mkdirs(new Path(localDir, subDir))
    "%s/%s/%s.jar".format(localDir, subDir, taskAttemptId)
  }



  /**
   * Copy necessary files to local (file) system so to speed up computation.
   * @param conf should contain related cache files if any.
   */
  protected def localize(conf: HamaConfiguration) = 
    CacheService.moveCacheToLocal(conf)
  
  /**
   * - Configure FileSystem's working directory with corresponded 
   * <b>task.getConfiguration()</b>.
   * - And add additional classpath to task's configuration.
   * @param conf is the common setting from bsp peer container.
   * @param task contains setting for particular job computation.
   */
  protected def settingFor(task: Task) = {
    val taskConf = task.getConfiguration
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

  protected def configSyncClient(task: Task) =  
    syncClient ! SetTaskAttemptId(task.getId)  

  /**
   * Internal sync to ensure all peers is registered/ ready.
   * @param superstep indicate the curent superstep value.
   */
  //TODO: should the task's superstep be confiured to 0 instead?
  protected def firstSync(task: Task) {
    Utils.await[BarrierMessage](syncClient, Enter(task.getCurrentSuperstep)) 
    Utils.await[BarrierMessage](syncClient, Leave(task.getCurrentSuperstep)) 
    task.increatmentSuperstep
  }
  /**
   * - Update task phase to sync 
   * - Enter barrier sync 
   */
  protected def enter: Receive = {
    case Enter(superstep) => {
      task.map { (aTask) => transitToSync(aTask) } // TODO: more detail phase
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
    case TransferredFailure => container ! TransferredFailure 
  }
 
  protected def leave: Receive = {
    case Leave(superstep) => syncClient ! Leave(superstep)
  }

 protected def exitBarrier: Receive = {
    case ExitBarrier => {
      // spawn checkpointer
      // checkpointer ! StartCheckpoint xxxxx
      // TODO: call to next superstep
    }
  }

  //protected def checkpoint() = 

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
  protected def getSuperstepCount: Receive = {
    case GetSuperstepCount => task.map { (aTask) => 
      sender ! aTask.getCurrentSuperstep
    }
  }

  protected def peerIndex: Receive = {
    case GetPeerIndex => task.map { (aTask) => 
      sender ! aTask.getId.getTaskID.getId 
    }
  }

  protected def send: Receive = {
    case Send(peerName, msg) => messenger ! Send(peerName, msg)
  }

  protected def getCurrentMessage: Receive = {
    case GetCurrentMessage => {
      clients ++= Map(GetCurrentMessage.toString -> sender)
      messenger ! GetCurrentMessage 
    }
  }

  /**
   * Reply client's (usually superstep) GetCurrentMessage.
   */
  protected def currentMessage: Receive = {
    case CurrentMessage(msg) => reply(GetCurrentMessage.toString, msg) 
  }

  protected def getNumCurrentMessages: Receive = {
    case GetNumCurrentMessages => {
      clients ++= Map(GetNumCurrentMessages.toString -> sender)
      messenger ! GetNumCurrentMessages 
    }
  }

  protected def numCurrentMessages: Receive = {
    case NumCurrentMessages(num) => reply(GetNumCurrentMessages.toString, num)
  }

  protected def reply(key: String, result: Any) = clients.find( 
    p => p._1.equals(key)
  ) match {
    case Some(found) => {
      val who = found._2
      LOG.debug("Reply {}'s {} with value {}.", who, key, result)
      who ! result
    }
    case None => LOG.error("Don't know who send message {}!", key)
  }

  protected def getPeerName: Receive = {
    case GetPeerName => {
      clients ++= Map(GetPeerName.toString -> sender)
      syncClient ! GetPeerName
    }
  }

  protected def peerName: Receive = {
    case PeerName(name) => reply(GetPeerName.toString, name)
  }

  protected def getPeerNameBy: Receive = {
    case GetPeerNameBy(index) => {
      clients ++= Map(GetPeerNameBy.getClass.getName -> sender)
      syncClient ! GetPeerNameBy(index)
    }
  } 

  protected def peerNameByIndex: Receive = {
    case PeerNameByIndex(name) => reply(GetPeerNameBy.getClass.getName, name)
  }

  protected def getNumPeers: Receive = {
    case GetNumPeers => {
      clients ++= Map(GetNumPeers.toString -> sender)
      syncClient ! GetNumPeers
    }
  }

  protected def numPeers: Receive = {
    case NumPeers(num) => reply(GetNumPeers.toString, num)
  }

  protected def getAllPeerNames: Receive = {
    case GetAllPeerNames => {
      clients ++= Map(GetAllPeerNames.toString -> sender)
      syncClient ! GetAllPeerNames
    }
  }

  protected def allPeerNames: Receive = {
    case AllPeerNames(allPeers) => reply(GetAllPeerNames.toString, allPeers)
  }

  /**
   * BSPPeer ask controller which task attempt id it is.
   */
  protected def taskAttemptId: Receive = {
    case GetTaskAttemptId => task.map { (aTask) => sender ! aTask.getId }
  }

  protected def clear() = messenger ! ClearOutgoingMessages

  protected def clearOutgoingMessages: Receive = {
    case Clear => clear
  }

  override def receive = customize orElse execute orElse enter orElse inBarrier orElse proxyBundleIterator orElse transferredCompleted orElse transferredFailure orElse leave orElse exitBarrier orElse getSuperstepCount orElse peerIndex orElse taskAttemptId orElse send orElse getCurrentMessage orElse currentMessage orElse getNumCurrentMessages orElse numCurrentMessages orElse getPeerName orElse peerName orElse getPeerNameBy orElse peerNameByIndex orElse getNumPeers orElse numPeers orElse getAllPeerNames orElse allPeerNames orElse unknown 
  
}
