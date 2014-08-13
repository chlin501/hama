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
import akka.actor.ActorSystem
import java.io.IOException
import java.net.InetAddress
import java.net.URLClassLoader
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Writable
import org.apache.hama.bsp.BSPJobID
import org.apache.hama.bsp.Counters
import org.apache.hama.bsp.TaskAttemptID
import org.apache.hama.bsp.RecordReader
import org.apache.hama.bsp.OutputCollector
import org.apache.hama.fs.CacheService
import org.apache.hama.fs.Operation
import org.apache.hama.HamaConfiguration
//import org.apache.hama.io.IO
import org.apache.hama.logging.Logger
import org.apache.hama.monitor.CheckpointerReceiver
import org.apache.hama.ProxyInfo
import org.apache.hama.sync.SyncException
import org.apache.hama.message.BSPMessageBundle
import org.apache.hama.message.MessageManager
import org.apache.hama.message.PeerCommunicator
import org.apache.hama.monitor.NoMoreMessages
import org.apache.hama.monitor.Pack
import org.apache.hama.monitor.SavePeerMessages
import org.apache.hama.monitor.SaveSuperstep
import org.apache.hama.sync.PeerSyncClient
import org.apache.hama.sync.SyncServiceFactory
import scala.collection.JavaConversions._
import scala.collection.immutable.Queue

private[v2] final case class TaskWithStats(task: Task, counters: Counters) {
  if(null == task)
    throw new IllegalArgumentException("Task is not provided.")
  if(null == counters) 
    throw new IllegalArgumentException("Counters is not provided!")
}

object Coordinator {

  /**
   * Create Coordinator with common configuration and child process's actor 
   * system.
   * @param conf is the common configuration.
   * @param actorSystem is the actor system created during process creation. 
   */
  def apply(conf: HamaConfiguration, 
            actorSystem: ActorSystem): Coordinator = 
    new Coordinator(conf, actorSystem)
}

/**
 * This class purely implements BSPPeer interface. With a separated 
 * @{link BSPPeerExecutor} serves for executing worker logic.
 *
 * {@link Coordinator} is responsible for providing related services, 
 * including:
 * - messenging
 * - io 
 * - sync
 * 
 * And update task information such as
 * - status
 * - start time
 * - finish time
 * - progress 
 * when necessary.
 * 
 * @param conf contains the common configuration.
 * @param bspActorSystem is the actor system of bsp process; it is responsible 
 *                       for launching peer messenger for coordinating between
 *                       peers.
 */
class Coordinator(conf: HamaConfiguration, 
                  bspActorSystem: ActorSystem) extends BSPPeer 
                                               with CheckpointerReceiver
                                               with Logger {

  /* task and counters specific to a particular v2.Job. */
  protected var taskWithStats: TaskWithStats = _

  /* services for a particular v2.Task. */
  protected var messenger = MessageManager.get[Writable](configuration)
  // TODO: we need a new io interface for reader/ writer which are not bound to
  //       key value pair.
  //protected var io = IO.get[_, _](configuration)
  protected val syncClient = SyncServiceFactory.getPeerSyncClient(configuration)

  private var allPeers: Array[String] = _

  override def configuration(): HamaConfiguration = conf

  override def getCommonConf(): HamaConfiguration = configuration

  override def currentTaskAttemptId(): String = getTask.getId.toString

  override def currentSuperstepCount(): Long = getSuperstepCount

  // only for internal use.
  private def getTask(): Task = taskWithStats match {
    case null => 
      throw new IllegalStateException("TaskWithStats is not yet setup!")
    case _ => taskWithStats.task
  }

  private def getCounters(): Counters = taskWithStats match {
    case null =>
      throw new IllegalStateException("TaskWithStats is not yet setup!")
    case _ => taskWithStats.counters 
  }

  /**
   * Configure necessary services for a specific task, including
   * - io
   * - sync
   * - messaging
   * Note: <pre>configuration != task.getConfiguration(). </pre>
   *       The formor comes from child process, the later from the task.
   * @param task contains setting for a specific job; its configuration differs
   *             from conf provided by {@link Container}.
   */
  protected[v2] def configureFor(task: Task) {
    this.taskWithStats = TaskWithStats(task, new Counters())
    this.messenger = messengingService(this.messenger, configuration, getTask) 
    //this.io = ioService[_, _](io, configuration, taskWithStats) 
    localize(configuration, getTask)
    settingForTask(configuration, getTask)
    syncService(configuration, getTask)
    //updateStatus(configuration, getTask)
    firstSync(getTask.getCurrentSuperstep)
  }

  /**
   * Internal sync to ensure all peers is registered/ ready.
   * @param superstep indicate the curent superstep value.
   */
  protected def firstSync(superstep: Long) {
    //TODO: should the task's superstep be confiured to 0 instead?
    syncClient.enterBarrier(getTask.getId.getJobID, getTask.getId, superstep)
    syncClient.leaveBarrier(getTask.getId.getJobID, getTask.getId, superstep)
    getTask.increatmentSuperstep
  }

  /** 
   * - Configure FileSystem's working directory with corresponded 
   * <b>task.getConfiguration()</b>.
   * - And add additional classpath to task's configuration.
   * @param conf is the common setting from bsp peer container.
   * @param task is contains setting for particular job computation.
   */
  protected def settingForTask(conf: HamaConfiguration, task: Task) {
    val taskConf = task.getConfiguration
    Operation.get(taskConf).setWorkingDirectory(
      new Path(Operation.defaultWorkingDirectory(taskConf))
    )
    val libjars = CacheService.moveJarsAndGetClasspath(conf) 
    libjars match {
      case null => LOG.warn("No jars to be included for "+task.getId)
      case _ => {
        LOG.info("Jars to be included in classpath are "+libjars.mkString(", "))
        taskConf.setClassLoader(new URLClassLoader(libjars, 
                                                   taskConf.getClassLoader))
      }
    } 
  }
  
  /**
   * Setup message service for a specific task.
   */
  protected def messengingService(old: MessageManager[Writable], 
                                  conf: HamaConfiguration, 
                                  task: Task): MessageManager[Writable] = {
    old match {
      case null => 
      case _ => old.close 
    }
    val mgr = MessageManager.get[Writable](conf)
    mgr.init(conf, task.getId)
    if(mgr.isInstanceOf[PeerCommunicator]) {
      mgr.asInstanceOf[PeerCommunicator].initialize(bspActorSystem)
    }
    mgr
  }

  /**
   * Setup io service for a specific task.
  protected def ioService[I, O](io: IO[I, O],
                                conf: HamaConfiguration, 
                                taskWithStats: TaskWithStats): IO[I, O] = {
    val newIO = io match {
      case null => IO.get[I,O](conf)
      case _ => io
    }
    newIO.initialize(getTask.getConfiguration, // tight to a task 
                     getTask.getSplit, 
                     getCounters)
    newIO
  }
   */

  /**
   * Copy necessary files to local (file) system so to speed up computation.
   * @param conf should contain related cache files if any.
   */
  protected def localize(conf: HamaConfiguration, task: Task) = 
    CacheService.moveCacheToLocal(conf)

  /**
   * Instantiate ZooKeeper sync service for BarrierSynchronization according to
   * a specific task.
   */
  protected def syncService(conf: HamaConfiguration, task: Task) {
    this.syncClient.init(conf, task.getId.getJobID, task.getId)
    this.syncClient.register(task.getId.getJobID, task.getId, host, port)
  }

  /**
   * The host this actor runs on. It may be different from the host that remote 
   * module listens to.
   * @return String name of the host.
   */
  protected def host(): String = 
    configuration.get("bsp.peer.hostname", 
                      InetAddress.getLocalHost.getHostName) 

  protected def port(): Int = configuration.getInt("bsp.peer.port", 61000)

  /**
   * TODO: task phase update needs to be done in e.g. BSPTask.java
   * Update current task status
   * (Async actor) report status back to master.
  def updateStatus(conf: HamaConfiguration, task: Task) {
  }
   */ 

  //override def getIO[I, O](): IO[I, O] = this.io.asInstanceOf[IO[I, O]]

  @throws(classOf[IOException])
  override def send(peerName: String, msg: Writable) = 
    messenger.send(peerName, msg.asInstanceOf[Writable])

  override def getCurrentMessage(): Writable = 
    messenger.getCurrentMessage.asInstanceOf[Writable]

  override def getNumCurrentMessages(): Int = messenger.getNumCurrentMessages

  @throws(classOf[SyncException])
  protected def enterBarrier() {
    syncClient.enterBarrier(getTask.getId.getJobID, 
                            getTask.getId, 
                            getTask.getCurrentSuperstep)
  }

  @throws(classOf[SyncException])
  protected def leaveBarrier() {
    syncClient.leaveBarrier(getTask.getId.getJobID, 
                            getTask.getId,
                            getTask.getCurrentSuperstep)
  }

  def doTransfer(peer: ProxyInfo, bundle: BSPMessageBundle[Writable]) {
    try {
      messenger.transfer(peer, bundle) 
    } catch {
      case ioe: IOException => 
        LOG.error("Fail transferring messages to {} for {}", 
                  peer, ioe)
    }
  }

  @throws(classOf[IOException])
  override def sync() {
    val pack = nextPack
    val it = messenger.getOutgoingBundles
    it match {
      case null =>
        throw new IllegalStateException("MessageManager's outgoing bundles is "+
                                        "null!")
      case _ => {
        asScalaIterator(it).foreach( entry => {
          val peer = entry.getKey
          val bundle = entry.getValue
          it.remove 
          saveBundle(pack, getTask.getId.toString, getSuperstepCount,
                     peer, bundle)
          doTransfer(peer, bundle)      
        })
        noMoreMessages(pack, getTask.getId.toString, getSuperstepCount)
      }
    }
     
    enterBarrier()
    messenger.clearOutgoingMessages
    checkpoint(pack)
    leaveBarrier()
    // TODO: record time elapsed between enterBarrier and leaveBarrier, etc.
    getTask.increatmentSuperstep
    //updateStatus(configuration, getTask) 
  } 

  protected def checkpoint(pack: Option[Pack]) {
    pack match {
      case Some(pack) => pack.ckpt match {
        case Some(ckpt) => {
          val className = pack.superstep.getClass.getName
          val variables = pack.superstep.getVariables
          ckpt ! SaveSuperstep(className, variables)
        }
        case None => 
          LOG.warn("Checkpointer not found!")
      }
      

      case None => LOG.warn("Checkpointer not found!")
    }
  }

  protected def noMoreMessages(pack: Option[Pack],
                               taskAttemptId: String,
                               superstepCount: Long) = pack match {
    case None => LOG.warn("Checkpointer for "+taskAttemptId+" at "+
                          superstepCount+" is missing!")
    case Some(pack) => pack.ckpt match {
      case Some(found) => found ! NoMoreMessages
      case None => LOG.warn("Checkpointer for "+taskAttemptId+" at "+
                            superstepCount+" is missing!")
    }
  }

  protected def saveBundle[M <: Writable](pack: Option[Pack], 
                                          taskAttemptId: String, 
                                          superstepCount: Long,
                                          peer: ProxyInfo, 
                                          bundle: BSPMessageBundle[M]) = 
    pack match {
      case None => 
        LOG.debug("TaskAttemptID "+taskAttemptId+" at "+ superstepCount+
                  " has checkpoint enabled? "+ isCheckpointEnabled)
      case Some(pack) => pack.ckpt match {
        case Some(found) => found ! SavePeerMessages[M](peer, bundle)
        case None => LOG.debug("TaskAttemptID "+taskAttemptId+" at "+ 
                               superstepCount+" has checkpoint enabled? "+ 
                               isCheckpointEnabled)
      }
    }
  
  override def getSuperstepCount(): Long = 
    getTask.getCurrentSuperstep

  private def initPeerNames() {
    if (null == allPeers) {
      allPeers = syncClient.getAllPeerNames(getTask.getId);
    }
  }

  override def getPeerName(): String = syncClient.getPeerName

  override def getPeerName(index: Int): String = {
    initPeerNames
    allPeers(index)
  }

  override def getPeerIndex(): Int = getTask.getId.getTaskID.getId

  override def getAllPeerNames(): Array[String] = {
    initPeerNames
    allPeers
  }

  override def getNumPeers(): Int = {
    initPeerNames
    allPeers.length
  }

  override def clear() = messenger.clearOutgoingMessages 

  override def getTaskAttemptId(): TaskAttemptID = getTask.getId

  /**
   * This is called after {@link BSP#bsp} finishs its execution in the end.
   * It will close all necessary operations.
   */
  protected[v2] def close() = {
   // TODO: close all operations, including io/ message/ sync/ local files in cache, etc.
    this.clear 
    this.syncClient.close
    this.messenger.close
  }

}
