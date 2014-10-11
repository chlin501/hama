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
import org.apache.hadoop.util.ReflectionUtils
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
import org.apache.hama.monitor.Checkpointer
import org.apache.hama.monitor.StartCheckpoint
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
//import org.apache.hama.sync.SetTaskAttemptId
import org.apache.hama.sync.SyncException
import org.apache.hama.sync.WithinBarrier
import org.apache.hama.util.Utils
import scala.collection.JavaConversions._
import scala.util.Try
import scala.util.Success
import scala.util.Failure

sealed trait TaskStatMessage
final case object GetSuperstepCount extends TaskStatMessage
final case object GetPeerIndex extends TaskStatMessage
final case object GetTaskAttemptId extends TaskStatMessage

sealed trait CoordinatorMessage
final case class Customize(task: Task) extends CoordinatorMessage
final case object Execute extends CoordinatorMessage
final case class InstantiationFailure(className: String, cause: Throwable)
      extends CoordinatorMessage
final case class SuperstepNotFoundFailure(className: String) 
      extends CoordinatorMessage
final case class FinishCleanup(superstepClassName: String)
      extends CoordinatorMessage 

/**
 * {@link Coordinator} is responsible for providing related services, 
 * including:
 * - messenging
 * - <strik>io</strike>
 * - sync
 */
// TODO: report task stats through container
//  - status
//  - start time
//  - finish time
//  - progress, etc.
class Coordinator(conf: HamaConfiguration,  // common conf
                  task: Task,
                  container: ActorRef, 
                  messenger: ActorRef, 
                  syncClient: ActorRef,
                  tasklog: ActorRef) extends LocalService with TaskLog {

  type ProxyAndBundleIt = Iterator[Entry[ProxyInfo, BSPMessageBundle[Writable]]]

  type ActorMessage = String

  protected[v2] var supersteps = Map.empty[String, ActorRef]

  //protected[v2] var superstepCleanupCount = 0

  protected[v2] var currentSuperstep: Option[ActorRef] = None

  /**
   * This holds messge to the asker's actor reference.
   * Once the target replies, find in cache and reply to the original target.
   */
  // TODO: perhaps replace with forward func instead.
  protected var clients = Map.empty[ActorMessage, ActorRef]


  override def LOG: LoggingAdapter = Logging[TaskLogger](tasklog)

  override def configuration(): HamaConfiguration = conf

  override def initializeServices() {
    localize(conf)
    settingFor(task)  
    firstSync(task)  
  }

  protected def execute: Receive = {
    case Execute => doExecute
  }

  protected def doExecute() {  // TODO: move to TaskController?
    val taskConf = task.getConfiguration
    val taskAttemptId = task.getId.toString
    LOG.info("Start configuring for task {}", taskAttemptId)
    addJarToClasspath(taskAttemptId, taskConf)
    setupSupersteps(taskConf)
    currentSuperstep = startSuperstep
  }

  protected def startSuperstep(): Option[ActorRef] =  
    execute(classOf[FirstSuperstep].getSimpleName, bspPeer, 
            Map.empty[String, Writable])

  /**
   * Cached supersteps is mapped from class simple name to spawned actor ref.
   * So the class name passed in should be 
   * {@link Superstep#getClass#getSimpleName}.
   * @param className is superstep class's simple name witout prefix, i.e., 
   *                  "superstep-", not spawned actor ref.
   * @param peer is the coordinator wrapped by {@link BSPPeerAdapter}.
   * @param variables is a cached map data from previous superstep.
   */
  protected def execute(className: String, // getClass.getSimpleName
                        peer: BSPPeer, // adaptor
                        variables: Map[String, Writable]): Option[ActorRef] = 
    supersteps.find(entry => if(entry._1.equals(className)) true else {
      entry._1.equals(className)
    }) match {
      case Some(found) => {
        val superstepActorRef = found._2
        beforeCompute(peer, superstepActorRef, variables)
        whenCompute(peer, superstepActorRef)
        afterCompute(peer, superstepActorRef)
        Option(superstepActorRef)
      }
      case None => {
        container ! SuperstepNotFoundFailure(className)
        None
      }
    }

  /**
   * This function calls {@link Superstep#next} in obtaining next superstep
   * class.
   */
  protected def nextSuperstepClass: Receive = {
    case NextSuperstepClass(next) => next match { 
      case null => eventually(bspPeer) 
      case clazz@_ => self ! Enter(task.getCurrentSuperstep) 
    }
  }

  protected def eventually(peer: BSPPeer) = cleanup(peer)

  protected def beginCleanup(peer: BSPPeer) = task.transitToCleanup

  protected def whenCleanup(peer: BSPPeer) = supersteps.foreach { case (k, v)=> 
    v ! Cleanup(peer)
  }

  def cleanup(peer: BSPPeer) {  
    beginCleanup(peer)
    whenCleanup(peer)
  }

  //endOfCleanup(peer)  xx wait for reply
  protected def finishCleanup: Receive = {
    case FinishCleanup(superstepClassName) => {
      // TODO: check if all superstep clean is called.
    }
  }

  protected def beforeCompute(peer: BSPPeer, superstep: ActorRef,
                              variables: Map[String, Writable]) {
    task.transitToCompute
    superstep ! SetVariables(variables)
  }

  protected def whenCompute(peer: BSPPeer, superstep: ActorRef) = 
    superstep ! Compute(peer) 

  protected def afterCompute(peer: BSPPeer, superstep: ActorRef) { } 
  
  /**
   * Setup {@link Superstep}s to be executed, including:
   * - Instantiate superstep classes by {@link ReflectionsUtils}.
   * - Spawn an actor that holds the superstep.
   * - Setup superstep by sending message to the actor.
   * - Cache the actor for flow control.
   * @param taskConf is specific configuration of a task.
   */
  protected def setupSupersteps(taskConf: HamaConfiguration) {
    val classes = taskConf.get("hama.supersteps.class")
    LOG.info("Supersteps {} will be instantiated!", classes)
    val classNames = classes.split(",")
    classNames.foreach( className => {
      instantiate(className, taskConf) match {
        case Success(superstep) => { 
          val spawned = spawn("superstep-"+className, classOf[SuperstepWorker],
                               superstep)
          spawned ! Setup(bspPeer) 
          supersteps ++= Map(className -> spawned)
        }
        case Failure(cause) => 
          container ! InstantiationFailure(className, cause)
      } 
    })  
  }

  protected def bspPeer(): BSPPeer = BSPPeerAdapter(task.getConfiguration, self)

  protected def instantiate(className: String, 
                            taskConf: HamaConfiguration): Try[Superstep] = {
    val loader = classWithLoader(className, taskConf)
    Try(ReflectionUtils.newInstance(loader, taskConf).asInstanceOf[Superstep])
  }

  /**
   * Create class with class loader containing jar new added to classpath.
   */
  protected def classWithLoader(className: String, 
                                taskConf: HamaConfiguration): Class[_] = 
    Class.forName(className, true, taskConf.getClassLoader)

  /**
   * Add jar, containing {@link Superstep} implementation, to classpath so that 
   * supersteps can be instantiated.
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

  //protected def configSyncClient(task: Task) =  
    //syncClient ! SetTaskAttemptId(task.getId)  

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
      transitToSync(task) 
      syncClient ! Enter(superstep) 
    }
  }

  // TODO: further divide task sync phase
  protected def transitToSync(task: Task) = task.transitToSync 

  /**
   * {@link PeerSyncClient} reply after passing `Enter' function.
   */
  protected def inBarrier: Receive = {
    case WithinBarrier => withinBarrier(task) 
  }

  protected def withinBarrier(task: Task) = getBundles()

  /**
   * Obtain message bundles sent by calling {@link BSPPeer#send} function.
   */
  protected def getBundles() = messenger ! GetOutgoingBundles

  /**
   * Messenger replies by supplying bundles in outgoing message queue.
   * Then message bundles is transmitted, wraped by iterator, to remote 
   * messenger.
   */
  protected def proxyBundleIterator: Receive = {
    case it: ProxyAndBundleIt => {
      transmit(it, task) 
      checkIfTransferredCompleted(task)
    }
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
    case TransferredCompleted => {
      beforeLeave
      self ! Leave(task.getCurrentSuperstep)
    }
  }

  /**
   * Clear outgoing queues, etc.
   */
  protected def beforeLeave() = clear

  protected def transferredFailure: Receive = {
    case TransferredFailure => container ! TransferredFailure 
  }
 
  protected def leave: Receive = {
    case Leave(superstep) => syncClient ! Leave(superstep)
  }

  /**
   * Spawn checkpointer.
   * Start checkpoint process.
   * Trigger next superstep.
   */
  protected def exitBarrier: Receive = {
    case ExitBarrier => {
      checkpoint
      // TODO: trigger/ call to next superstep
      beforeNextSuperstep
    }
  }

  protected def checkpoint() = createCheckpointer.map { (checkpointer) =>
    checkpointer ! StartCheckpoint  
  }

  protected def beforeNextSuperstep() = retrieveVariables 

  protected def retrieveVariables() = currentSuperstep.map { (current) => 
    current ! GetVariables
  }

  protected def createCheckpointer(): Option[ActorRef] = currentSuperstep.map {
    (current) => spawn("checkpointer-"+task.getCurrentSuperstep+"-"+
                       task.getId.toString, classOf[Checkpointer], 
                       conf, task.getConfiguration, task.getId, 
                       task.getCurrentSuperstep, messenger, current) 
  }

  /**
   * This function retrieves {@link Superstep} map variables object for 
   * next superstep execution.
   */
  protected def variables: Receive = {
    case Variables(variables) => currentSuperstep.map { (current) => 
      currentSuperstep = execute(superstepClassName(current.path.name), bspPeer,
                                 variables)
    }
  }

  protected def superstepClassName(actorName: String): String = {
    val start = actorName.indexOf("-") + 1
    actorName.substring(start, actorName.length)
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
  protected def getSuperstepCount: Receive = {
    case GetSuperstepCount => sender ! task.getCurrentSuperstep
  }

  protected def peerIndex: Receive = {
    case GetPeerIndex => sender ! task.getId.getTaskID.getId 
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
    case GetTaskAttemptId => sender ! task.getId 
  }

  protected def clear() = messenger ! ClearOutgoingMessages

  protected def clearOutgoingMessages: Receive = {
    case Clear => clear
  }

  override def receive = execute orElse enter orElse inBarrier orElse proxyBundleIterator orElse transferredCompleted orElse transferredFailure orElse leave orElse exitBarrier orElse getSuperstepCount orElse peerIndex orElse taskAttemptId orElse send orElse getCurrentMessage orElse currentMessage orElse getNumCurrentMessages orElse numCurrentMessages orElse getPeerName orElse peerName orElse getPeerNameBy orElse peerNameByIndex orElse getNumPeers orElse numPeers orElse getAllPeerNames orElse allPeerNames orElse nextSuperstepClass orElse variables orElse unknown 
  
}
