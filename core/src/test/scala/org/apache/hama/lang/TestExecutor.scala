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
package org.apache.hama.lang

import akka.actor.Actor
import akka.actor.ActorRef
import akka.event.Logging
import akka.actor.ActorSystem
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.apache.hama.TestEnv
import org.apache.hama.HamaConfiguration
import org.apache.hama.groom.BSPPeerContainer
import org.apache.hama.groom.ContainerReady
import org.apache.hama.groom.ContainerStopped
import org.apache.hama.groom.MockContainer
import org.apache.hama.groom.ShutdownSystem
import org.apache.hama.groom.TaskManager
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

final case class Add(e: ActorRef)

class Aggregator(conf: HamaConfiguration, tester: ActorRef) 
      extends TaskManager(conf) {

  var e1_notified = false
  var e2_notified = false
  var e3_notified = false

  var executors = Set.empty[ActorRef]

  override def initializeServices { }

  def add: Receive = {
    case Add(e) => {
      executors += e
      LOG.info("{} is added. Now executors size is {}", 
               e.path.name, executors.size)
    }
  }

  def reg: Receive = {
    case "register" => {
      executors.foreach( e => e ! Register)
      LOG.info("Register all executors to BSPPeerContainers ...")
    }
  }

  def fork: Receive = {
    case "fork" => {
      executors.foreach( e => e.path.name match {
        case "groomServer_executor_1" => { 
          e ! Fork(1, configuration); Thread.sleep(3*1000)
        }
        case "groomServer_executor_2" => {
          e ! Fork(2, configuration); Thread.sleep(3*1000)
        }
        case "groomServer_executor_3" => {
          e ! Fork(3, configuration); Thread.sleep(3*1000)
        }
        case rest@_ => 
          throw new RuntimeException("Unknown executor "+rest) 
      })
    }
  }

  def readyx: Receive = {
    case ContainerReady => {
      LOG.info("Container {} replies it's ready!", sender.path.name)
      tester ! sender.path.name+"_ready"
    }
  }

  def stopAll: Receive = {
    case "stopAll" => {
      executors.foreach( e =>  e ! StopProcess)
      LOG.info("Send StopProcess message to all BSPPeerCotnainer ...")
    }  
  }

  def stopped: Receive = {
    case ContainerStopped => tester ! sender.path.name+"_container_stopped"
  }

  def shut: Receive = {
    case "shutdown" => {
      executors.foreach( e => e ! ShutdownSystem)
    }
  }

  override def receive = shut orElse add orElse reg orElse fork orElse readyx orElse stopAll orElse stopped orElse super.receive
} 

object WithRemoteSetting {
  def toConfig: Config = {
    ConfigFactory.parseString("""
      akka {
        actor {
          provider = "akka.remote.RemoteActorRefProvider"
          serializers {
            java = "akka.serialization.JavaSerializer"
            proto = "akka.remote.serialization.ProtobufSerializer"
            writable = "org.apache.hama.io.serialization.WritableSerializer"
          }
          serialization-bindings {
            "com.google.protobuf.Message" = proto
            "org.apache.hadoop.io.Writable" = writable
          }
        }
        remote {
          netty.tcp {
            hostname = "127.0.0.1" 
            port = 50000
          }
        }
      }
    """)
  }
} 

@RunWith(classOf[JUnitRunner])
class TestExecutor extends TestEnv(ActorSystem("TestExecutor", 
                                               WithRemoteSetting.toConfig)) {

  override protected def beforeAll = {
    super.beforeAll
    testConfiguration.set("bsp.working.dir", testRoot.getCanonicalPath)
    testConfiguration.setClass("bsp.child.class", classOf[MockContainer],
                               classOf[BSPPeerContainer])
  }

  def createProcess(name: String): ActorRef = {
    LOG.info("Create actor "+name+" ...")
    createWithArgs(name, classOf[Executor], testConfiguration)
  }

  it("test forking a process") {
    LOG.info("Test forking a process...")
    val e1 = createProcess("groomServer_executor_1")
    val e2 = createProcess("groomServer_executor_2")
    val e3 = createProcess("groomServer_executor_3")
    val taskManagerName = 
      testConfiguration.get("bsp.groom.taskmanager.name", "taskManager")
    val aggregator = createWithTester(taskManagerName, classOf[Aggregator]) 
    aggregator ! Add(e1) 
    aggregator ! Add(e2) 
    aggregator ! Add(e3) 

    aggregator ! "register"
  
    aggregator ! "fork"

    LOG.info("Wait 20 seconds for child process being started up.")
    sleep(20.seconds)
  
    expectAnyOf("groomServer_executor_1_ready", "groomServer_executor_2_ready",
                "groomServer_executor_3_ready")

    expectAnyOf("groomServer_executor_1_ready", "groomServer_executor_2_ready",
                "groomServer_executor_3_ready")

    expectAnyOf("groomServer_executor_1_ready", "groomServer_executor_2_ready",
                "groomServer_executor_3_ready")

    LOG.info("Wait 3 seconds before calling stopAll.")
    sleep(3.seconds)

    aggregator ! "stopAll"

    LOG.info("Wait 3 seconds for child process to be stopped.")
    sleep(3.seconds)

    expectAnyOf("groomServer_executor_1_container_stopped", 
                "groomServer_executor_2_container_stopped", 
                "groomServer_executor_3_container_stopped")

    expectAnyOf("groomServer_executor_1_container_stopped", 
                "groomServer_executor_2_container_stopped", 
                "groomServer_executor_3_container_stopped")

    expectAnyOf("groomServer_executor_1_container_stopped", 
                "groomServer_executor_2_container_stopped", 
                "groomServer_executor_3_container_stopped")

    aggregator ! "shutdown"

    sleep(3.seconds)

    LOG.info("Done!")
  }
}
