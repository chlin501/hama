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
import akka.actor.Props
import akka.event.Logging
import akka.actor.ActorSystem
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.apache.hama.bsp.v2.Task
import org.apache.hama.master.Directive
import org.apache.hama.master.Directive.Action
import org.apache.hama.master.Directive.Action._
import org.apache.hama.groom.BSPPeerContainer
import org.apache.hama.groom.ContainerReady
import org.apache.hama.groom.ContainerStopped
import org.apache.hama.groom.KillAck
import org.apache.hama.groom.LaunchAck
import org.apache.hama.groom.MockContainer
import org.apache.hama.groom.ResumeAck
import org.apache.hama.groom.ShutdownSystem
import org.apache.hama.groom.TaskManager
import org.apache.hama.HamaConfiguration
import org.apache.hama.TestEnv
import org.apache.hama.util.JobUtil
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

class Aggregator(conf: HamaConfiguration, tester: ActorRef) 
      extends TaskManager(conf) {

  override def name: String = "mockTaskManager"

  override def initializeServices {
    initializeSlots(getMaxTasks)
    LOG.info("Done initializing {} with {} slots ...", self, getMaxTasks)
  }

  // LaunchAck(2,attempt_test_0001_000007_2)
  override def preLaunchAck(ack: LaunchAck) {
    LOG.info("{} receives {}", name, ack)
    tester ! ack.taskAttemptId.toString
  }

  // ResumeAck(1,attempt_test_0003_000001_1)
  override def preResumeAck(ack: ResumeAck) {
    LOG.info("{} receives {}", name, ack)
    tester ! ack.taskAttemptId.toString
  }

/*
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
*/

  override def receive = super.receive
} 

object WithRemoteSetting {
  def toConfig: Config = {
    ConfigFactory.parseString("""
      testExecutor {
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
      }
    """)
  }
} 

@RunWith(classOf[JUnitRunner])
class TestExecutor extends TestEnv(ActorSystem("TestExecutor", 
                                               WithRemoteSetting.toConfig.
                                               getConfig("testExecutor"))) 
                   with JobUtil {

  override protected def beforeAll = {
    super.beforeAll
    testConfiguration.setBoolean("bsp.tasks.log.console", true)
    testConfiguration.set("bsp.working.dir", testRoot.getCanonicalPath)
    testConfiguration.set("bsp.groom.actor-system.name", "TestExecutor")
    testConfiguration.setClass("bsp.child.class", classOf[MockContainer],
                               classOf[BSPPeerContainer])
  }

  def createDirective(action: Directive.Action, task: Task): Directive = 
    new Directive(action, task, "testMaster")

  it("test forking a process") {
    LOG.info("Test forking a process...")

    val taskManagerName = 
      testConfiguration.get("bsp.groom.taskmanager.name", "taskManager")

    val aggregator = createWithTester(taskManagerName, classOf[Aggregator]) 
  
    /* jobid, taskId, taskAttemptId, partition */
    val task1 = createTask("test", 1, 7, 2, 7) 
    val directive1 = createDirective(Launch, task1)
    aggregator ! directive1

    val task2 = createTask("test", 3, 1, 1, 9) 
    val directive2 = createDirective(Resume, task2)
    aggregator ! directive2

    //val task3 = createTask("test", 1, 4, 3, 2) 
    //val directive3 = createDirective(Kill, task3)
    //aggregator ! directive3

    sleep(5.seconds)

    expectAnyOf("attempt_test_0001_000007_2", "attempt_test_0003_000001_1")

    // TODO: 1. issue kill action, then verify
    //       2. stop/ shutdown the system
/*

    LOG.info("Wait 3 seconds before calling stopAll.")
    sleep(3.seconds)

    aggregator ! "stopAll"

    LOG.info("Wait 10 seconds for child process to be stopped.")
    sleep(10.seconds)

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

    sleep(10.seconds)
*/

    LOG.info("Done!")
  }
}
