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

import akka.actor.ActorSystem
import java.io.IOException
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
import org.apache.hama.logging.Logger
import org.apache.hama.io.IO
//import org.apache.hama.io.DefaultIO
import org.apache.hama.message.MessageManager
import org.apache.hama.message.PeerInfo
import org.apache.hama.sync.PeerSyncClient
import org.apache.hama.sync.SyncServiceFactory

private[v2] final case class TaskWithStats(task: Task, counters: Counters) {
  if(null == task)
    throw new IllegalArgumentException("Task is not provided.")
  if(null == counters) 
    throw new IllegalArgumentException("Counters is not provided!")
}

/**
 * This class purely implements BSPPeer interface. With a separated 
 * @{link BSPPeerExecutor} serves for executing worker logic.
 * {@link BSPPeerCoordinator} is responsible for providing related services, 
 * including
 * - messenging
 * - io 
 * - sync
 * And update task information such as
 * - status
 * - start time
 * - finish time
 * - progress 
 * when necessary.
 */
class BSPPeerCoordinator(bspActorSystem: ActorSystem) extends BSPPeer 
                                                      with Logger {

  /* common setting for the entire BSPPeer. */
  protected var configuration: HamaConfiguration = _
  /* task and counters specific to a particular v2.Job. */
  protected var taskWithStats: TaskWithStats = _

  /* services for a particular v2.Task. */
  protected var messenger: MessageManager[Writable] = _
  //protected var io: IO[org.apache.hama.bsp.RecordReader[_,_], org.apache.hama.bsp.OutputCollector[_, _]] = _
  protected var syncClient: PeerSyncClient = _  

  private var allPeers: Array[String] = _

  /**
   * Initialize necessary services, including
   * - io
   * - sync
   * - messaging
   * Note: <pre>conf != task.getConfiguration(). </pre>
   *       The formor comes from process startup, the later from task.
   * @param conf contains common setting for starting up related services.
   * @param task contains setting for a specific job; its configuration differs
   *             from conf provided by {@link BSPPeerContainer}.
   */
  protected[v2] def initialize(conf: HamaConfiguration, task: Task) {
    this.configuration = conf
    this.taskWithStats = TaskWithStats(task, new Counters())
    this.messenger = messengingService(conf, taskWithStats.task) 
    //this.io = ioService(conf, taskWithStats) 
    localize(conf, taskWithStats.task)
    settingForTask(conf, taskWithStats.task)
    this.syncClient = syncService(conf, taskWithStats.task)
    //updateStatus(conf, taskWithStats.task)
    doSync()
  }

  /**
   * Internal sync to ensure all peers is registered/ ready.
   */
  protected def doSync() {
//TODO: 
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
    if(null != libjars) 
      LOG.info("Classpath to be included are "+libjars.mkString(", "))
    taskConf.setClassLoader(new URLClassLoader(libjars, 
                                               taskConf.getClassLoader))
  }

  
  /**
   * Setup message service according to a specific task.
   */
  protected def messengingService(conf: HamaConfiguration, task: Task): 
      MessageManager[Writable] = {
    val mgr = MessageManager.get[Writable](conf)
    mgr.init(conf, task.getId)
    mgr
  }

  /**
   * Setup io service according to a specific task.
  protected def ioService(conf: HamaConfiguration, 
                          taskWithStats: TaskWithStats): 
      IO[RecordReader[_,_], OutputCollector[_,_]] = {
    val io = IO.get[RecordReader[_,_], OutputCollector[_,_]](conf)
    io.initialize(taskWithStats.task.getConfiguration, // tight to a task 
                  taskWithStats.task.getSplit, 
                  taskWithStats.counters)
    io
  }
   */

  /**
   * Copy necessary files to local (file) system so to speed up computation.
   * @param conf should contain related cache files if any.
   */
  protected def localize(conf: HamaConfiguration, task: Task) = 
    CacheService.moveCacheToLocal(conf)

  protected def syncService(conf: HamaConfiguration, task: Task): 
      PeerSyncClient =  {
    val client = SyncServiceFactory.getPeerSyncClient(conf)
    client.init(conf, task.getId.getJobID, task.getId)
    client.register(task.getId.getJobID, task.getId, host, port)
    client
  }

// beg TODO: move to PeerInfo
  protected def peerInfo: PeerInfo = PeerInfo(peerActorSystem, host, port)

  protected def peerActorSystem: String = 
    "BSPPeerSystem%s".format(configuration.getInt("bsp.child.slot.seq", 1))

  protected def host(): String = 
    configuration.get("bsp.peer.hostname", "0.0.0.0") 

  protected def port(): Int = configuration.getInt("bsp.peer.port", 61000)

  protected def socketAddress(): String = "%s:%s".format(host, port)
// end TODO: move to PeerInfo

  //def updateStatus(conf: HamaConfiguration) {}

  //override def getIO[org.apache.hama.bsp.RecordReader[_,_], org.apache.hama.bsp.OutputCollector[_,_]](): IO[org.apache.hama.bsp.RecordReader[_,_], org.apache.hama.bsp.OutputCollector[_,_]] = this.io

  @throws(classOf[IOException])
  override def send(peerName: String, msg: Writable) = 
    messenger.send(peerName, msg.asInstanceOf[Writable])

  override def getCurrentMessage(): Writable = 
    messenger.getCurrentMessage.asInstanceOf[Writable]

  override def getNumCurrentMessages(): Int = messenger.getNumCurrentMessages

  @throws(classOf[IOException])
  override def sync() {
    val it = messenger.getOutgoingBundles
    while(it.hasNext) {
      val entry = it.next
      val addr = entry.getKey
      val bundle = entry.getValue
      it.remove
      // actual send message out 
    }
  } 

  override def getSuperstepCount(): Long = 
    taskWithStats.task.getCurrentSuperstep

  private def initPeerNames() {
    if (null == allPeers) {
      allPeers = syncClient.getAllPeerNames(taskWithStats.task.getId);
    }
  }

  override def getPeerName(): String = socketAddress

  override def getPeerName(index: Int): String = {
    initPeerNames
    allPeers(index)
  }

  override def getPeerIndex(): Int = taskWithStats.task.getId.getTaskID.getId

  override def getAllPeerNames(): Array[String] = {
    initPeerNames
    allPeers
  }

  override def getNumPeers(): Int = {
    initPeerNames
    allPeers.length
  }

  override def clear() = messenger.clearOutgoingMessages 

  override def getConfiguration(): HamaConfiguration = configuration

  override def getTaskAttemptId(): TaskAttemptID = taskWithStats.task.getId

}
