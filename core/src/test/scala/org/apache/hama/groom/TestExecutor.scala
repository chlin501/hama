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

import akka.actor.ActorRef
import akka.actor.ActorSystem
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.apache.hama.bsp.v2.Task
import org.apache.hama.master.Directive
import org.apache.hama.master.Directive.Action
import org.apache.hama.master.Directive.Action._
import org.apache.hama.HamaConfiguration
import org.apache.hama.TestEnv
import org.apache.hama.conf.Setting
import org.apache.hama.util.JobUtil
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

class Aggregator(setting: Setting, groom: ActorRef, reporter: ActorRef, 
                 tester: ActorRef) 
      extends TaskCounsellor(setting, groom, reporter) {

  var command: Int = _

  private def increament = command += 1
  private def decrement = command -= 1
  private def isZero: Boolean = (command == 0)

  override def initializeServices {
    initializeSlots(getMaxTasks)
    LOG.info("Done initializing {} with {} slots ...", self, getMaxTasks)
  }

  // LaunchAck(2,attempt_test_0001_000007_2)
  override def postLaunchAck(ack: LaunchAck) {
    LOG.debug("<LaunchAck> {} receives {}.\nCurrent slots {}.", name, ack, slots)
    tester ! ack.taskAttemptId.toString
    increament
  }

  // ResumeAck(1,attempt_test_0003_000001_1)
  override def postResumeAck(ack: ResumeAck) {
    LOG.debug("<ResumeAck> {} receives {}.\nCurrent slots {}.", name, ack, slots)
    tester ! ack.taskAttemptId.toString
    increament
  }

  override def postKillAck(ack: KillAck) {
    LOG.debug("<KillAck> {} receives {}.\nCurrent slots {}", name, ack, slots)
    tester ! ack.taskAttemptId.toString
  }

  override def postContainerStopped(executor: ActorRef) {
    LOG.info("Executor {} is stopped.\nCurrent slots {}", 
              executor.path.name, slots)
    tester ! executor.path.name
    decrement
    if(isZero) {
      slots.foreach( slot => 
        tester ! slot.task
      )
    }
  }

  override def receive = super.receive
} 

// TODO: change inherit from conf.Setting  
object SettingX {
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
class TestExecutor extends TestEnv("TestExecutor", 
                                   SettingX.toConfig.getConfig("testExecutor"))
                   with JobUtil {

  val groom: ActorRef = null

  override protected def beforeAll = {
    super.beforeAll

  }

  def createDirective(action: Directive.Action, task: Task): Directive = 
    new Directive(action, task, "testMaster")

  def config(conf: HamaConfiguration) {
    conf.setBoolean("bsp.tasks.log.console", true)
    conf.set("bsp.working.dir", testRoot.getCanonicalPath)
    conf.set("groom.actor-system.name", "TestExecutor")
    conf.setClass("bsp.child.class", classOf[MockContainer],
                               classOf[Container])
  }

  it("test forking processes") {
    LOG.info("Test forking processes...")

    val setting = Setting.groom
    config(setting.hama)

    val taskCounsellorName = 
      setting.hama.get("groom.taskcounsellor.name", "taskCounsellor")

    val reporter = createWithArgs("reporter", classOf[Reporter], setting)
    val aggregator = createWithArgs(taskCounsellorName, classOf[Aggregator],
                                    setting, groom, reporter, tester) 
  
    /* jobid, taskId, taskAttemptId */
    val task1 = createTask("test", 1, 7, 2) 
    val directive1 = createDirective(Launch, task1)  // launch task
    aggregator ! directive1

    val task2 = createTask("test", 3, 1, 1) 
    val directive2 = createDirective(Resume, task2) // resume task
    aggregator ! directive2

    sleep(15.seconds)

    expectAnyOf("attempt_test_0001_000007_2", "attempt_test_0003_000001_1")
    expectAnyOf("attempt_test_0001_000007_2", "attempt_test_0003_000001_1")

    // kill previous actions.
    val directive3 = createDirective(Kill, task1)
    aggregator ! directive3
    val directive4 = createDirective(Kill, task2)
    aggregator ! directive4

    expectAnyOf("attempt_test_0001_000007_2", "attempt_test_0003_000001_1")
    expectAnyOf("attempt_test_0001_000007_2", "attempt_test_0003_000001_1")

    aggregator ! StopExecutor(1)
    aggregator ! StopExecutor(2)
    aggregator ! StopExecutor(3)

    sleep(20.seconds)

    expectAnyOf("groomServer_executor_1", "groomServer_executor_2") 
    expectAnyOf("groomServer_executor_1", "groomServer_executor_2") 
    expect(None)
    expect(None)
    expect(None)

    LOG.info("Done TestExecutor!")
  }
}
