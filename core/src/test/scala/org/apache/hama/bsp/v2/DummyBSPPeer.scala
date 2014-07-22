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
import org.apache.hama.bsp.TaskAttemptID
import org.apache.hama.HamaConfiguration
import org.apache.hama.logging.Logger

/**
 * This is intended to be used in test purpose only.
 */
protected class DummyBSPPeer extends BSPPeer with Logger {

  @throws(classOf[IOException])
  override def send(peerName: String, msg: Writable) = 
    LOG.info(getClass.getSimpleName+" starts sending message ...") 

  @throws(classOf[IOException])
  override def getCurrentMessage(): Writable = null.asInstanceOf[Writable]

  override def getNumCurrentMessages(): Int = -1

  @throws(classOf[IOException])
  override def sync() = LOG.info(getClass.getSimpleName+" starts sync() ...")

  override def getSuperstepCount(): Long = -1

  override def getPeerName(): String = getClass.getSimpleName 

  override def getPeerName(index: Int): String = getClass.getSimpleName

  override def getPeerIndex(): Int = -1

  override def getAllPeerNames(): Array[String] = Array(getClass.getSimpleName)
 
  override def getNumPeers(): Int = -1

  override def clear() { }

  override def configuration(): HamaConfiguration = new HamaConfiguration

  override def getTaskAttemptId(): TaskAttemptID = 
    null.asInstanceOf[TaskAttemptID]

}
