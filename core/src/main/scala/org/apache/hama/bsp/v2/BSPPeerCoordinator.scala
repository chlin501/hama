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
import org.apache.hama.bsp.TaskAttemptID
import org.apache.hama.bsp.RecordReader
import org.apache.hama.bsp.OutputCollector
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
 */
class BSPPeerCoordinator extends BSPPeer {

  protected var configuration: HamaConfiguration = _
  protected var task: Task = _
  protected var messenger: MessageManager[_] = _
  protected var io: IO[RecordReader[_,_], OutputCollector[_,_]] = _
  protected var syncClient: PeerSyncClient = _ 

  /**
   * Initialize necessary services, including
   * - io
   * - sync
   * - messaging
   * @param conf contains related setting to startup related services.
   */
  protected[this] def initialize(conf: HamaConfiguration, task: Task) {
    this.configuration = conf
    this.task = task
    // messaging
    this.messenger = MessageManagerFactory.getMessageManager(conf) 
    this.messenger.init(configuration, getTaskAttemptId)
    // io
    this.io = ReflectionUtils.newInstance( 
      conf.getClassByName(conf.get("bsp.io.class",
                                   classOf[DefaultIO].getCanonicalName)), 
      conf).asInstanceOf[IO[RecordReader[_,_], OutputCollector[_,_]]]
    // sync
    this.syncClient = SyncServiceFactory.getPeerSyncClient(conf)
    syncClient.init(conf, task.getId.getJobID, task.getId)
    syncClient.register(task.getId.getJobID, task.getId,  
                        this.configuration.get("bsp.peer.hostname", "0.0.0.0"),
                        this.configuration.getInt("bsp.peer.port", 61000))
  }

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
