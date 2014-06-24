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

import java.io.IOException
import org.apache.hadoop.io.Writable
import org.apache.hadoop.util.ReflectionUtils
import org.apache.hama.bsp.BSPJobID
import org.apache.hama.bsp.Counters
import org.apache.hama.bsp.TaskAttemptID
import org.apache.hama.bsp.RecordReader
import org.apache.hama.bsp.OutputCollector
import org.apache.hama.fs.CacheService
import org.apache.hama.HamaConfiguration
import org.apache.hama.io.IO
import org.apache.hama.io.DefaultIO
import org.apache.hama.message.MessageManager
import org.apache.hama.message.MessageManagerFactory
import org.apache.hama.sync.PeerSyncClient
import org.apache.hama.sync.SyncServiceFactory

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
class BSPPeerCoordinator extends BSPPeer {

  /* common setting for the entire BSPPeer. */
  protected var configuration: HamaConfiguration = _
  /* specific to a particular v2.Job. */
  protected var task: Task = _ 
  /* counters and task is a pair. 1 to 1 relation. */ 
  protected var counters: Counters = _  // TODO: tmp. need to refactor.
  /* services for a particular v2.Task. */
  protected var messenger: MessageManager[_] = _
  protected var io: IO[RecordReader[_,_], OutputCollector[_,_]] = _
  protected var syncClient: PeerSyncClient = _ 

  /**
   * Initialize necessary services, including
   * - io
   * - sync
   * - messaging
   * Note: <pre>conf != task.getConfiguration(). </pre>
   *       The formor comes from process startup, the later from task.
   * @param conf contains related setting to startup related services.
   * @param task contains setting for a specific job; its configuration differs
   *             from conf provided by {@link BSPPeerContainer}.
   */
  protected[v2] def initialize(conf: HamaConfiguration, task: Task) {
    this.configuration = conf
    this.task = task
    this.counters = new Counters() // TODO: tmp. need to refator. 
    setupOperation(conf, task)
    this.messenger = messengingService(conf, task) 
    this.io = ioService(conf, task) 
    localize(conf, task)
    this.syncClient = syncService(conf, task)
    //updateStatus(conf, task)
  }

  /** 
   * Configure FileSystem's working directory with corresponded 
   * <b>task.getConfiguration()</b>.
   */
  protected def setupOperation(conf: HamaConfiguration, task: Task) {
   
  }
  
  /**
   * Setup message service according to a specific task.
   */
  protected def messengingService(conf: HamaConfiguration, task: Task): 
      MessageManager[_] = {
    val msgr = MessageManagerFactory.getMessageManager(conf) 
    msgr.init(conf, task.getId)
    msgr
  }

  protected def ioService(conf: HamaConfiguration, task: Task): 
      IO[RecordReader[_,_], OutputCollector[_,_]] = {
    val readerWriter = ReflectionUtils.newInstance( 
      conf.getClassByName(conf.get("bsp.io.class",
                                   classOf[DefaultIO].getCanonicalName)), 
      conf).asInstanceOf[IO[RecordReader[_,_], OutputCollector[_,_]]]
    readerWriter.initialize(task.getConfiguration, task.getSplit, counters) 
    readerWriter
  }

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

  protected def host(): String = 
    configuration.get("bsp.peer.hostname", "0.0.0.0") 

  protected def port(): Int = configuration.getInt("bsp.peer.port", 61000)

  protected def socketAddress(): String = "%s:%s".format(host, port)

  //def updateStatus(conf: HamaConfiguration) {}

  override def getIO(): IO[RecordReader[_,_], OutputCollector[_,_]] = io

  @throws(classOf[IOException])
  override def send(peerName: String, msg: Writable) {}

  override def getCurrentMessage(): Writable = null.asInstanceOf[Writable]

  override def getNumCurrentMessages(): Int = -1

  @throws(classOf[IOException])
  override def sync() {}

  override def getSuperstepCount(): Long = -1

  override def getPeerName(): String  = null 

  override def getPeerName(index: Int): String = null 

  override def getPeerIndex(): Int = -1

  override def getAllPeerNames(): Array[String] = null

  override def getNumPeers(): Int = -1

  override def clear() {}

  override def getConfiguration(): HamaConfiguration = configuration

  override def getTaskAttemptId(): TaskAttemptID = task.getId
  

}
