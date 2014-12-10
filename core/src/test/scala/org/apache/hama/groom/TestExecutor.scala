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
import com.typesafe.config.Config
import org.apache.hama.Agent
import org.apache.hama.HamaConfiguration
import org.apache.hama.TestEnv
import org.apache.hama.bsp.v2.Task
import org.apache.hama.conf.Setting
import org.apache.hama.master.Directive
import org.apache.hama.master.Directive.Action
import org.apache.hama.master.Directive.Action._
import org.apache.hama.util.JobUtil
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

// groom
class MockG extends Agent {

  override def receive = unknown
}

// reporter
class MockR extends Agent {

  override def receive = unknown

}

class Aggregator(setting: Setting, groom: ActorRef, reporter: ActorRef, 
                 tester: ActorRef) 
      extends TaskCounsellor(setting, groom, reporter) {

  var command: Int = 0

  private def increament = command += 1
  private def decrement = command -= 1
  private def isZero: Boolean = (command == 0)

  override def initializeServices {
    initializeSlots(maxTasks)
    LOG.info("Done initializing {} with {} slots ...", name, maxTasks)
  }

  // LaunchAck(2,attempt_test_0001_000007_2)
  override def postLaunchAck(ack: LaunchAck) {
    LOG.debug("<LaunchAck> {} receives {} with current slots {}.", 
              name, ack, slots)
    tester ! ack.taskAttemptId.toString
    increament
  }

  // ResumeAck(1,attempt_test_0003_000001_3)
  override def postResumeAck(ack: ResumeAck) {
    LOG.debug("<ResumeAck> {} receives {} with current slots {}.", 
              name, ack, slots)
    tester ! ack.taskAttemptId.toString
    increament
  }

  override def postKillAck(ack: KillAck) {
    LOG.debug("<KillAck> {} receives {} with current slots {}", 
              name, ack, slots)
    tester ! ack.taskAttemptId.toString
  }

  override def postContainerStopped(executor: ActorRef) {
    LOG.info("Executor {} is stopped with current slots {}", 
              executor.path.name, slots)
    tester ! executor.path.name
    decrement
    if(isZero) slots.foreach( slot => 
      tester ! slot.taskAttemptId
    )
  }

  override def receive = super.receive
} 

object TestExecutor {

  def content(): String = """
    groom {
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
  """

  def config(): Config = Setting.toConfig(content).getConfig("groom")

} 

@RunWith(classOf[JUnitRunner])
class TestExecutor extends TestEnv("TestExecutor", TestExecutor.config)
                   with JobUtil {

  override protected def beforeAll = super.beforeAll

  def newDirective(action: Directive.Action, task: Task): Directive = 
    new Directive(action, task, "testMaster")

  def config(conf: HamaConfiguration) {
    conf.setBoolean("bsp.tasks.log.console", true)
    conf.set("bsp.working.dir", testRoot.getCanonicalPath)
    conf.set("groom.actor-system.name", "TestExecutor")
    conf.setClass("bsp.child.class", classOf[MockContainer],
                               classOf[Container])
  }

  it("test task management and executor ...") {

    val setting = Setting.groom
    config(setting.hama)

    val taskCounsellorName = TaskCounsellor.simpleName(setting.hama)

    val groom = createWithArgs("mockGroom", classOf[MockG])
    val reporter = createWithArgs("mockReporter", classOf[MockR])
    val aggregator = createWithArgs(taskCounsellorName, classOf[Aggregator],
                                    setting, groom, reporter, tester) 
  
    /* jobid, taskId, taskAttemptId */
    val task1 = createTask("test", 1, 7, 2) 
    val directive1 = newDirective(Launch, task1)  // launch task
    aggregator ! directive1

    val task2 = createTask("test", 3, 1, 3) 
    val directive2 = newDirective(Resume, task2) // resume task
    aggregator ! directive2

    sleep(15.seconds)

    expectAnyOf("attempt_test_0001_000007_2", "attempt_test_0003_000001_3")
    expectAnyOf("attempt_test_0001_000007_2", "attempt_test_0003_000001_3")

    // kill previous actions.
    val directive3 = newDirective(Kill, task1)
    aggregator ! directive3
    val directive4 = newDirective(Kill, task2)
    aggregator ! directive4

    expectAnyOf("attempt_test_0001_000007_2", "attempt_test_0003_000001_3") // TODO: fail at here
    expectAnyOf("attempt_test_0001_000007_2", "attempt_test_0003_000001_3")

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
