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

object MockContainer {

  def main(args: Array[String]) = {
    val (sys, conf, seq)= BSPPeerContainer.initialize(args)
    BSPPeerContainer.launch(sys, classOf[MockContainer], conf, seq)
  }
}

class MockContainer(conf: HamaConfiguration) extends BSPPeerContainer(conf) {

  override def executorPath: String = {
    val actorSystemName = conf.get("bsp.groom.actor-system.name", 
                                   "TestExecutor")
    val port = conf.getInt("bsp.groom.actor-system.port", 50000)
    val host = conf.get("bsp.groom.actor-system.host", "127.0.0.1")
    val seq = conf.getInt("bsp.child.slot.seq", 1)
    val addr = ("akka.tcp://%1$s@%2$s:%3$d/user/taskManager/" +
               "groomServer_executor_%4$s").format(actorSystemName, 
                                                   host, 
                                                   port,
                                                   seq)
    LOG.info("Mock executor path to be looked up is at {}", addr)
    addr
  }
 
  override def receive = super.receive
}