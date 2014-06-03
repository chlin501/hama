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

import akka.actor.ActorContext
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Cancellable
import akka.event.Logging
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import java.io.File
import java.io.FileWriter
import org.apache.hama.bsp.TaskAttemptID
import org.apache.hama.bsp.v2.Task
import org.apache.hama.HamaConfiguration
import org.apache.hama.LocalService

object MockContainer {

  def main(args: Array[String]) = BSPPeerContainer.main(args)

}

class MockContainer(conf: HamaConfiguration) extends BSPPeerContainer(conf) {

  override def executorPath: String = {
    val addr = "akka.tcp://TestExecutor@127.0.0.1:50000/user/taskManager/" +
               "groomServer_executor_1"
    LOG.info("Mock executor path is at {}", addr)
    addr
  }
 
  override def receive = super.receive
}
