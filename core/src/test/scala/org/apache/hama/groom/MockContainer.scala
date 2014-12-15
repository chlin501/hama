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

/**
 * For TestExecutor.
 */
object MockContainer {

  def main(args: Array[String]) = {
    val parameters = Container.initialize(args)
    parameters.setting.hama.setClass("container.main", 
                                     classOf[MockContainer], 
                                     classOf[Container])
    Container.launchFrom(parameters)
  }
}

final case class MockExecutorLocator(conf: HamaConfiguration)

/**
 * For TestExecutor
 */
class MockContainer(setting: Setting, slotSeq: Int) 
      extends Container(setting, slotSeq) with ActorLocator {

  import scala.language.implicitConversions

  implicit def locateMockExecutor(mock: MockExecutorLocator) = 
      new ActorPathMagnet {
    type Path = String
    def apply(): Path = {
      val seq = mock.conf.getInt("bsp.child.slot.seq", 1)
      val counsellorName = TaskCounsellor.simpleName(mock.conf)
      val executorName = Executor.simpleName(mock.conf, seq)
      val actorSystemName = mock.conf.get("groom.actor-system.name", 
                                     "TestExecutor")
      val port = mock.conf.getInt("groom.port", 50000)
      val host = mock.conf.get("groom.host", "127.0.0.1")
      val addr = ("akka.tcp://%s@%s:%d/user/" + counsellorName + "/" +
                  executorName).format(actorSystemName, host, port)

      LOG.info("Mock executor path to be looked up is at {}", addr)
      addr  
    }
  }

  override def initializeServices {
    lookup(ExecutorName, locate(MockExecutorLocator(setting.hama)))
  }
 
  override def receive = super.receive
}
