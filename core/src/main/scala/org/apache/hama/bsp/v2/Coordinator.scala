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
import org.apache.hama.message.Send
import org.apache.hama.message.SetCoordinator
import org.apache.hama.message.Transfer
import org.apache.hama.message.TransferredCompleted
import org.apache.hama.message.TransferredFailure
import org.apache.hama.message.TransferredState
import org.apache.hama.monitor.Checkpointer
import org.apache.hama.monitor.GetLocalQueueMsgs
import org.apache.hama.monitor.GetMapVarNextClass
import org.apache.hama.sync.AllPeerNames
import org.apache.hama.sync.PeerClientMessage
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
final case class Spawned(superstep: Superstep, actor: ActorRef)

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

  protected[v2] var supersteps = Map.empty[String, Spawned]

  protected[v2] var currentSuperstep: Option[ActorRef] = None

  protected[v2] var nextSuperstep: Option[Class[_]] = None


  // TODO: move to Utils.time() function
  var start: Long = 0 
  var start_superstep: Long = 0 
  var finished_superstep: Long = 0 
  var elapsed_superstep: Long = 0 

  var start_enter: Long = 0
  var finished_enter: Long = 0
  var elapsed_enter: Long = 0

  var start_in: Long = 0
  var finished_in: Long = 0
  var elapsed_in: Long = 0

  var start_leave: Long = 0
  var finished_leave: Long = 0
  var elapsed_leave: Long = 0

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
    configMessenger
  }

  override def beforeRestart(what: Throwable, message: Option[Any]) =
    initializeServices

  protected def configMessenger() = messenger ! SetCoordinator(self)

  /**
   * Entry point to start task computation. 
   * TODO: move execution flow logic to controller?
   */
  protected def execute: Receive = {
    case Execute => doExecute
  }

  protected def doExecute() {  // TODO: move to TaskController?
    start = System.currentTimeMillis // test
    val s = System.currentTimeMillis 
    val taskConf = task.getConfiguration
    val taskAttemptId = task.getId.toString
    LOG.debug("Start configuring for task {}", taskAttemptId)
    addJarToClasspath(taskAttemptId, taskConf)
    setupPhase
    setupSupersteps(taskConf)
    currentSuperstep = startSuperstep
    LOG.debug("Start executing superstep {} for task {}", 
             currentSuperstep.getOrElse(null), taskAttemptId)
    val f = System.currentTimeMillis
    val e = f - s
    LOG.debug("[{}] Execute time -> Finished: {}, start: {}, elapsed: {}, "+
             "{} secs", task.getId, f, s, e, (e/1000d) ) 
  }

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
    LOG.info("Jar path for task {} in task configuration: {}", taskAttemptId, 
             jar)
    jar match {
      case null|"" => None
      case remoteUrl@_ => {
        val operation = Operation.get(taskConf)
        // TODO: change working directory? see bsppeerimpl or taskworker
        val localJarPath = createLocalPath(taskAttemptId, taskConf, operation) 
        operation.copyToLocal(new Path(remoteUrl))(new Path(localJarPath))
        LOG.info("Remote file {} is copied to {} for task {}", remoteUrl, 
                 localJarPath, taskAttemptId) 
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
   * Setup {@link Superstep}s to be executed, including:
   * - Instantiate superstep classes by {@link ReflectionsUtils}.
   * - Spawn an actor that holds the superstep.
   * - Setup superstep by sending message to the actor.
   * - Cache the actor for flow control.
   * @param taskConf is specific configuration of a task.
   */
  protected def setupSupersteps(taskConf: HamaConfiguration) {
    val classes = taskConf.get("hama.supersteps.class")
    LOG.info("Supersteps {} will be instantiated for task {}!", classes, 
             task.getId)
    val classNames = classes.split(",")
    classNames.foreach( className => {
      instantiate(className, taskConf) match {
        case Success(superstep) => { 
          val actor = spawn("superstep-"+className, classOf[SuperstepWorker],
                               superstep, self)
          actor ! Setup(bspPeer) 
          supersteps ++= Map(className -> Spawned(superstep, actor))
        }
        case Failure(cause) => 
          container ! InstantiationFailure(className, cause)
      } 
    })  
  }
  protected def startSuperstep(): Option[ActorRef] =  
    execute(classOf[FirstSuperstep].getName, bspPeer, 
            Map.empty[String, Writable])

  protected def isFirstSuperstep(className: String): Boolean =
    classOf[FirstSuperstep].getName.equals(className)

  /**
   * Cached supersteps is mapped from class simple name to spawned actor ref.
   * So the class name passed in should be 
   * {@link Superstep#getClass#getSimpleName}.
   * @param className is superstep class's full name witout prefix, i.e., 
   *                  "superstep-", not spawned actor ref.
   * @param peer is the coordinator wrapped by {@link BSPPeerAdapter}.
   * @param variables is a cached map data from previous superstep.
   */
  protected def execute(className: String, // getClass.getName
                        peer: BSPPeer, // adaptor
                        variables: Map[String, Writable]): Option[ActorRef] = 
    supersteps.find(entry => isFirstSuperstep(className) match {
      case true => {
        val instance = entry._2.superstep
        LOG.debug("Firstsuperstep instance {}, className {} for task {}", 
                  instance, className, task.getId)
        instance.isInstanceOf[FirstSuperstep]
      }  
      case false => {
        LOG.debug("Non first superstep key {}, className {} for task {}", 
                 entry._1, className, task.getId)
        entry._1.equals(className) 
      }
    }) match {
      case Some(found) => {
        LOG.debug("Superstep {} for task {} will be executed.", found._1, 
                  task.getId)
        val superstepActorRef = found._2.actor
        beforeCompute(peer, superstepActorRef, variables)
        whenCompute(peer, superstepActorRef)
        afterCompute(peer, superstepActorRef)
        Option(superstepActorRef)
      }
      case None => {
        LOG.error("Can't execute, superstep {} is missing for task {}!", 
                  className, task.getId)
        container ! SuperstepNotFoundFailure(className)
        None
      }
    }

  protected def beforeCompute(peer: BSPPeer, superstep: ActorRef,
                              variables: Map[String, Writable]) = 
    superstep ! SetVariables(variables)

  protected def whenCompute(peer: BSPPeer, superstep: ActorRef) {
    computePhase
    superstep ! Compute(peer) 
  }

  protected def afterCompute(peer: BSPPeer, superstep: ActorRef) { } 
  

  /**
   * This function is called by SuperstepWorker after finishing
   * {@link Superstep#compute} supplied with {@link Superstep#next} as next
   * {@link Superstep}.
   */
  protected def nextSuperstepClass: Receive = {
    case NextSuperstepClass(next) => next match { 
      case null => {
        nextSuperstep = None
        eventually(bspPeer) 
      }
      case clazz@_ => {
        start_superstep = System.currentTimeMillis
        start_enter = System.currentTimeMillis
        nextSuperstep = Option(clazz) 
        self ! Enter(task.getCurrentSuperstep) 
      }
    }
  }

  protected def eventually(peer: BSPPeer) = cleanup(peer)

  def cleanup(peer: BSPPeer) {  
    cleanupPhase
    whenCleanup(peer)
    val finished = System.currentTimeMillis
    val elapsed = finished - start
    LOG.info("Total time spent for executing task {} => Finished: {}, "+
             "start: {}, elapsed: {}, {} secs", task.getId, finished, start, 
             elapsed, (elapsed/1000d)) 
  }

  protected def whenCleanup(peer: BSPPeer) = supersteps.foreach { case (k, v)=> 
    v.actor ! Cleanup(peer)
  }

  //endOfCleanup(peer)  wait for reply
  protected def finishCleanup: Receive = {
    case FinishCleanup(superstepClassName) => {
      // TODO: check if all superstep clean is called.
    }
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
      case null => LOG.warning("No jars to be included for task {}", task.getId)
      case _ => {
        LOG.info("Jars to be included in classpath for task {} are {}", 
                 task.getId, libjars.mkString(", "))
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
    Utils.await[PeerClientMessage](syncClient, Enter(task.getCurrentSuperstep)) 
    Utils.await[PeerClientMessage](syncClient, Leave(task.getCurrentSuperstep)) 
    task.increatmentSuperstep
  }

  /**
   * - Update task phase to sync 
   * - Enter barrier sync 
   */
  protected def enter: Receive = {
    case Enter(superstep) => {
      barrierEnterPhase
      LOG.debug("Before calling BarrierClient's enter func for task {}", 
                task.getId)
      syncClient ! Enter(superstep) 
      LOG.debug("After calling BarierClient's enter func for task {}", 
                task.getId)
    }
  }

  /**
   * {@link PeerSyncClient} reply after passing `Enter' function.
   */
  protected def inBarrier: Receive = {
    case WithinBarrier => withinBarrier(task) 
  }

  protected def withinBarrier(task: Task) = {
    withinBarrierPhase
    finished_enter = System.currentTimeMillis
    elapsed_enter = finished_enter - start_enter
    LOG.debug("Time spent in Enter for task {} => Finished: {}, start: {}, "+
              "elapsed: {}, {} secs", task.getId, finished_enter, start_enter, 
              elapsed_enter, (elapsed_enter/1000d))
    start_in = System.currentTimeMillis
    transfer
    //getBundles()
  }

  /**
   * Ask messenger transfers messages to remote and notify when completed.
   */
  protected def transfer() = messenger ! Transfer

  /**
   * Obtain message bundles sent by calling {@link BSPPeer#send} function.
  protected def getBundles() = messenger ! GetOutgoingBundles
   */

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
    case Leave(superstep) => {
      finished_in = System.currentTimeMillis
      elapsed_in = finished_in - start_in 
      LOG.debug("Time spent in WithinBarrier for task {} => finished: {}, "+
                "start: {}, elapsed: {}, {} secs", task.getId, finished_in, 
                start_in, elapsed_in, (elapsed_in/1000d))
      barrierLeavePhase
      start_leave = System.currentTimeMillis 
      syncClient ! Leave(superstep)
    } 
  }

  /**
   * Spawn checkpointer.
   * Start checkpoint process.
   * Trigger next superstep.
   */
  protected def exitBarrier: Receive = {
    case ExitBarrier => {
      exitBarrierPhase
      finished_leave = System.currentTimeMillis
      elapsed_leave = finished_leave - start_leave
      LOG.debug("Time spent in Leave for task {} => finished: {}, start: {},"+
                "elapsed: {}, {} secs", task.getId, finished_leave, start_leave,
                elapsed_leave, (elapsed_leave/1000d))
      checkpoint
      beforeNextSuperstep
    }
  }

  /**
   * Send GetlocalQueueMsgs and GetMapVarNextClass to messenger and superstep
   * worker by coordinator to ensure the checkpoint data would be obtained 
   * before next computation is started. 
   */
  protected def checkpoint() = isCheckpoint match  {
    case true => createCheckpointer.map { (checkpointer) =>
      messenger ! GetLocalQueueMsgs(checkpointer)
      currentSuperstep.map { (current) => 
        current ! GetMapVarNextClass(checkpointer)
      } 
    }
    case false => 
  }

  protected def isCheckpoint(): Boolean = {
    val isEnabled = conf.getBoolean("bsp.checkpoint.enabled", true)
    LOG.debug("Is checkpoint enabled for task {}? {}", task.getId, isEnabled)
    isEnabled
  }
  
  protected def checkpointerName = 
    "checkpointer-"+task.getCurrentSuperstep+"-"+task.getId.toString 

  protected def createCheckpointer(): Option[ActorRef] = currentSuperstep.map {
    (current) => spawn(checkpointerName,
                       classOf[Checkpointer], 
                       conf, // common conf
                       task.getConfiguration, 
                       task.getId, 
                       task.getCurrentSuperstep.asInstanceOf[Long], 
                       messenger, 
                       current) 
  }

  protected def beforeNextSuperstep() = retrieveVariables 

  protected def retrieveVariables() = currentSuperstep.map { (current) => 
    LOG.debug("Current superstep for task {} before retrieving variable map {}",
              task.getId, current.path.name)
    current ! GetVariables
  }

  /**
   * This function retrieves {@link Superstep} map variables object for 
   * next superstep execution.
   */
  protected def variables: Receive = {
    case Variables(variables) => nextSuperstep.map { (next) => 
      finished_superstep = System.currentTimeMillis
      elapsed_superstep = finished_superstep - start_superstep
      LOG.debug("Time spent in single superstep for task {} => finished: {}, "+
               "start: {}, elapsed: {}, {} secs", task.getId, 
               finished_superstep, start_superstep, elapsed_superstep, 
               (elapsed_superstep/1000d))
      beforeExecuteNext(next)
      currentSuperstep = execute(next.getName, bspPeer, variables)
      afterExecuteNext(next)
      LOG.debug("Superstep for task {} to be executed: {}", task.getId,
                next.getName)
    }
  }

  protected def beforeExecuteNext(clazz: Class[_]) { }


  protected def afterExecuteNext(clazz: Class[_]) { }

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
      LOG.debug("Reply {}'s {} with value {} by task {}.", who, key, result,
                task.getId)
      who ! result
    }
    case None => LOG.error("Don't know who send message {} for task {}!", key,
                           task.getId)
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

  /**
   * Clear outgoing mssage queue, which stores messages, generated by the 
   * current superstep, to be sent out to remote.
   */
  protected def clear() = messenger ! ClearOutgoingMessages

  protected def clearOutgoingMessages: Receive = {
    case Clear => clear
  }

  // task phase switch
  protected def setupPhase() = task.setupPhase

  protected def computePhase() = task.computePhase

  protected def barrierEnterPhase() = task.barrierEnterPhase

  protected def withinBarrierPhase() = task.withinBarrierPhase

  protected def barrierLeavePhase() = task.barrierLeavePhase

  protected def exitBarrierPhase() = task.exitBarrierPhase

  protected def cleanupPhase() = task.cleanupPhase

  override def receive = execute orElse enter orElse inBarrier orElse transferredCompleted orElse transferredFailure orElse leave orElse exitBarrier orElse getSuperstepCount orElse peerIndex orElse taskAttemptId orElse send orElse getCurrentMessage orElse currentMessage orElse getNumCurrentMessages orElse numCurrentMessages orElse getPeerName orElse peerName orElse getPeerNameBy orElse peerNameByIndex orElse getNumPeers orElse numPeers orElse getAllPeerNames orElse allPeerNames orElse nextSuperstepClass orElse variables orElse unknown 
  
}
