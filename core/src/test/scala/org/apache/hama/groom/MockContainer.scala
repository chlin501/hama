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
package org.apache.hama.groom

import org.apache.hama.HamaConfiguration
import org.apache.hama.conf.Setting
import org.apache.hama.util.ActorLocator
import org.apache.hama.util.ActorPathMagnet

final case class MockTaskCounsellorLocator(conf: HamaConfiguration)

/**
 * For TestExecutor
 */
class MockContainer(setting: Setting, slotSeq: Int) 
      extends Container(setting, slotSeq) with ActorLocator {

  import scala.language.implicitConversions

  implicit def locateMockTaskCounsellor(mock: MockTaskCounsellorLocator) = 
      new ActorPathMagnet {
    type Path = String
    def apply(): Path = {
      val counsellorName = TaskCounsellor.simpleName(mock.conf)
      val actorSystemName = mock.conf.get("groom.actor-system.name", 
                                     "TestExecutor")
      val host = mock.conf.get("groom.host", "127.0.0.1")
      val port = mock.conf.getInt("groom.port", 50000)
      val addr = "akka.tcp://%s@%s:%d/user/%s".format(actorSystemName, host, 
                                                      port, counsellorName)

      LOG.info("Mock task counsellor path to be looked up is at {}", addr)
      addr  
    }
  }

  override def initializeServices {
    lookup(TaskCounsellorName, locate(MockTaskCounsellorLocator(setting.hama)))
  }
 
  override def receive = super.receive
}
