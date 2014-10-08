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
import org.apache.hadoop.io.Writable
import org.apache.hama.HamaConfiguration
import org.apache.hama.Clear
import org.apache.hama.bsp.TaskAttemptID
import org.apache.hama.message.GetCurrentMessage
import org.apache.hama.message.GetNumCurrentMessages
import org.apache.hama.message.Send
import org.apache.hama.sync.GetAllPeerNames
import org.apache.hama.sync.GetNumPeers
import org.apache.hama.sync.GetPeerName
import org.apache.hama.sync.GetPeerNameBy
import org.apache.hama.util.Utils

object BSPPeerAdapter {

  /**
   * Adapter for BSPPeer. Will be used by {@link Superstep}.
   */
  def apply(taskConf: HamaConfiguration, coordinator: ActorRef): 
    BSPPeerAdapter = new BSPPeerAdapter(taskConf, coordinator)

}

class BSPPeerAdapter(taskConf: HamaConfiguration, coordinator: ActorRef) 
    extends BSPPeer {

  @throws(classOf[IOException])
  override def send(peerName: String, msg: Writable) = 
    coordinator ! Send(peerName, msg)

  @throws(classOf[IOException])
  override def getCurrentMessage(): Writable = 
    Utils.await[Writable](coordinator, GetCurrentMessage)

  override def getNumCurrentMessages(): Int = 
    Utils.await[Int](coordinator, GetNumCurrentMessages)

  override def getSuperstepCount(): Long = 
    Utils.await[Long](coordinator, GetSuperstepCount)
  
  override def getPeerName(): String = 
    Utils.await[String](coordinator, GetPeerName)

  override def getPeerName(index: Int): String = 
    Utils.await[String](coordinator, GetPeerNameBy(index))

  override def getPeerIndex(): Int = Utils.await[Int](coordinator, GetPeerIndex)
  
  override def getAllPeerNames(): Array[String] =
    Utils.await[Array[String]](coordinator, GetAllPeerNames)
  
  override def getNumPeers(): Int = Utils.await[Int](coordinator, GetNumPeers)
  
  override def clear() = coordinator ! Clear
  
  override def configuration(): HamaConfiguration = taskConf

  override def getTaskAttemptId(): TaskAttemptID = 
    Utils.await[TaskAttemptID](coordinator, GetTaskAttemptId)

}
