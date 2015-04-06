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
import org.apache.hama.bsp.TaskAttemptID
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
import org.apache.hama.monitor.TaskReport
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
final case class InstantiationFailure(id: TaskAttemptID, cause: Throwable) 
    extends RuntimeException(cause) with CoordinatorMessage
final case class SuperstepNotFoundFailure(className: String) 
      extends CoordinatorMessage
final case class CleanupFinished(superstepClassName: String)
      extends CoordinatorMessage 
final case class Spawned(superstep: Superstep, actor: ActorRef)
final case class TaskFinished(taskAttemptId: String) extends CoordinatorMessage

object Coordinator {

  val emptyVariables = Map.empty[String, Writable]
}

/**
 * {@link Coordinator} is responsible for providing related services, 
 * including:
 * - messenging
 * - sync
 */
// TODO: coordinator life cycle trait e.g. startExecute, whenFinished -> notify container? etc.
class Coordinator(conf: HamaConfiguration,  // common conf
                  task: Task,
                  container: ActorRef, 
                  messenger: ActorRef, 
                  syncClient: ActorRef,
                  tasklog: ActorRef) extends LocalService with TaskLog {

  import Coordinator._

  type ProxyAndBundleIt = Iterator[Entry[ProxyInfo, BSPMessageBundle[Writable]]]

  type ActorMessage = String

  /**
   * Map from superstep name to the superstep instance and its holder (actor).
   */
  protected[v2] var supersteps = Map.empty[String, Spawned]

  /**
   * Superstep actor that is currently running.
   */
  protected[v2] var currentSuperstep: Option[ActorRef] = None

  /**
   * Superstep class to be executed next.
   */
  protected[v2] var nextSuperstep: Option[Class[_]] = None

  /**
   * This is applied for check if all supersteps finish cleanup phase.
   */
  protected[v2] var cleanupCount = 0
  
  /**
   * This holds messge to the asker's actor reference.
   * Once the target replies, find in cache and reply to the original target.
   */
  // TODO: perhaps replace with forward func instead.
  protected var clients = Map.empty[ActorMessage, ActorRef]

  override def LOG: LoggingAdapter = Logging[TaskLogger](tasklog)

  override def initializeServices() {
    localize(conf)
    settingFor(task)  
    firstSync(task)  
    configMessenger
    // startExecute // TODO: container will restored data from ckpt. then coordinator will be inited with restored data supplied. then coordinator will start executeFrom (superstep, variables) left in the last checkpoint.
  }

  protected def configMessenger() = messenger ! SetCoordinator(self)

  protected def startExecute() {  
    task.markTaskStarted
    val taskConf = task.getConfiguration
    val taskAttemptId = task.getId.toString
    LOG.debug("Start configuring for task {}", taskAttemptId)
    addJarToClasspath(taskAttemptId, taskConf)
    setupPhase
    setupSupersteps(taskConf)
    currentSuperstep = startSuperstep 
    LOG.debug("Start executing superstep {} for task {}", 
              currentSuperstep.getOrElse(null), taskAttemptId)
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
          // TODO: save task as well so after recovery sys knows where to restart 
          val actor = spawn("superstep-"+className, classOf[SuperstepWorker],
                            /* task, */superstep, self)
          actor ! Setup(bspPeer) 
          supersteps ++= Map(className -> Spawned(superstep, actor))
        }
        /* container will restart this task becuase it's not yet executed. */
        case Failure(cause) => throw InstantiationFailure(task.getId, cause)  
      } 
    })  
  }

  /**
   * Execute computation logic from a specific superstep.
   */
  protected def executeFrom(superstepName: String,
                            variables: Map[String, Writable] = emptyVariables): 
    Option[ActorRef] = execute(superstepName, bspPeer, variables)

  protected def startSuperstep(): Option[ActorRef] = {
    val ref =execute(classOf[FirstSuperstep].getName, bspPeer, 
            Map.empty[String, Writable])
    runningState
    ref
  }

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
  protected def execute(className: String, 
                        peer: BSPPeer, 
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
      case None => throw new RuntimeException("Superstep "+className+ " is "+
                                              "missing for task "+ task.getId +
                                              "!") 
      /*{
        LOG.error("Can't execute, superstep {} is missing for task {}!", 
                  className, task.getId)
        failedState
        container ! SuperstepNotFoundFailure(className)
        None
      }*/
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
   * Once SuperstepWorker finishing its compute function 
   * {@link Superstep#compute} will send this message with 
   * {@link Superstep#next} supplied as next {@link Superstep}.
   */
  protected def nextSuperstepClass: Receive = {
    case NextSuperstepClass(next) => next match { 
      case null => {
        nextSuperstep = None
        eventually(bspPeer) 
      }
      case clazz@_ => {
        nextSuperstep = Option(clazz) 
        self ! Enter(task.getCurrentSuperstep) 
      }
    }
  }

  protected def eventually(peer: BSPPeer) = cleanup(peer)

  def cleanup(peer: BSPPeer) {  
    cleanupPhase
    whenCleanup(peer)
  }


  protected def whenCleanup(peer: BSPPeer) = supersteps.foreach { case (k, v)=> 
    v.actor ! Cleanup(peer)
  }

  protected def cleanupFinished: Receive = {
    case CleanupFinished(superstepClassName) => {
      cleanupCount += 1   
      if(cleanupCount == supersteps.size) { 
        succeedState
        task.markTaskFinished
        notifyContainer(task.getId.toString) 
        context.children foreach context.stop
        context.stop(self) 
      }
    }
  }

  protected def notifyContainer(taskAttemptId: String) =
    container ! TaskFinished(taskAttemptId) 

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

  /**
   * Internal sync to ensure all peers is registered/ ready.
   * @param superstep indicate the curent superstep value.
   */
  //TODO: should the task's superstep be confiured to 0 instead?
  protected def firstSync(task: Task) {
LOG.info("###### first time syncing to zk BEG ... ")
    Utils.await[PeerClientMessage](syncClient, Enter(task.getCurrentSuperstep)) 
    Utils.await[PeerClientMessage](syncClient, Leave(task.getCurrentSuperstep)) 
    task.incrementSuperstep
LOG.info("###### first time syncing to zk END ... ")
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
    transfer
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
    case TransferredFailure => throw new RuntimeException(
      "Unable to transfer messages for task "+task.getId+"!" 
    )
    /*{
      failedState
      container ! TransferredFailure 
    }*/
  }
 
  protected def leave: Receive = {
    case Leave(superstep) => {
      barrierLeavePhase
      syncClient ! Leave(superstep)
    } 
  }

  /**
   * Sync client leaves barrier synchronization.
   * Spawn checkpointer.
   * Start checkpoint process.
   * Trigger next superstep.
   */
  protected def exitBarrier: Receive = {
    case ExitBarrier => {
      exitBarrierPhase
      checkpoint
      beforeNextSuperstep
    }
  }

  /**
   * Send GetlocalQueueMsgs and GetMapVarNextClass to messenger and superstep
   * worker by coordinator to ensure the checkpoint data would be obtained 
   * before next computation is started. 
   */
  protected def checkpoint() = isCheckpointEnabled match  {
    case true => createCheckpointer.map { (checkpointer) =>
      messenger ! GetLocalQueueMsgs(checkpointer)
      currentSuperstep.map { (current) => 
        current ! GetMapVarNextClass(checkpointer)
      } 
    }
    case false => 
  }

  protected def isCheckpointEnabled(): Boolean = {
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
  override def whenClose() = {
    clear 
    syncClient ! Close 
    messenger ! Close
    super.whenClose
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

  /**
   * BSPPeerAdapter, on behalf of Superstep, asks for messages sent to it.
   */
  protected def getCurrentMessage: Receive = {
    case GetCurrentMessage => { 
      clients ++= Map(GetCurrentMessage.toString -> sender)
      messenger ! GetCurrentMessage 
    }
  }

  /**
   * Reply client's - Superstep - GetCurrentMessage.
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
  protected def setupPhase() {
    task.setupPhase
    reportTask
  }

  protected def computePhase() {
    task.computePhase
    reportTask
  }

  protected def barrierEnterPhase() {
    task.barrierEnterPhase
    reportTask
  }

  protected def withinBarrierPhase() {
    task.withinBarrierPhase
    reportTask
  }

  protected def barrierLeavePhase() {
    task.barrierLeavePhase
    reportTask
  }

  /**
   * Change task phase.
   * Increment superstep count by one.
   * Report the task to master.
   */
  protected def exitBarrierPhase() {
    task.exitBarrierPhase
    task.incrementSuperstep
    reportTask
  }

  protected def cleanupPhase() {
    task.cleanupPhase
    reportTask
  }

  // task state
  protected def waitingState() {
    task.waitingState
    reportTask
  }

  protected def runningState() {
    task.runningState
    reportTask
  }

  protected def succeedState() {
    task.succeededState
    reportTask
  }

/*
  protected def failedState() { 
    //task.failedState instead of reporting failure, exception is thrown!
    reportTask 
  }

  protected def killedState() {
    task.killedState
    reportTask
  }

  protected def stoppedState() {
    task.stoppedState
    reportTask
  }
*/

  protected def reportTask() = container ! TaskReport(task.copyTask)

  /**
   * Close all services after this actor is stopped.
   */
  override def stopServices = whenClose

  override def receive = enter orElse inBarrier orElse transferredCompleted orElse transferredFailure orElse leave orElse exitBarrier orElse getSuperstepCount orElse peerIndex orElse taskAttemptId orElse send orElse getCurrentMessage orElse currentMessage orElse getNumCurrentMessages orElse numCurrentMessages orElse getPeerName orElse peerName orElse getPeerNameBy orElse peerNameByIndex orElse getNumPeers orElse numPeers orElse getAllPeerNames orElse allPeerNames orElse nextSuperstepClass orElse variables orElse cleanupFinished orElse unknown 
  
}
