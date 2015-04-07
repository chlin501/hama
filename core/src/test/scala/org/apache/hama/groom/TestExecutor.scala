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
import org.apache.hama.Mock
import org.apache.hama.TestEnv
import org.apache.hama.bsp.v2.Task
import org.apache.hama.conf.Setting
import org.apache.hama.master.Directive
import org.apache.hama.master.Directive.Action
import org.apache.hama.master.Directive.Action._
import org.apache.hama.util.JobUtil
import org.apache.hama.util.Utils
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

class MockGroom1(setting: Setting, reporter: ActorRef, tester: ActorRef) 
      extends Mock {

  protected var counsellor: Option[ActorRef] = None

  override def preStart = {
    counsellor = Option(spawn(TaskCounsellor.simpleName(setting), 
      classOf[MockTaskCounsellor], setting, self, reporter, tester))
    LOG.info("Spawning task counsellor {}", counsellor)
  }

  protected def msgs: Receive = {
    case d: Directive => counsellor.map { c => c forward d }
  }
 
  override def receive = msgs orElse super.receive
}

class MockTaskCounsellor(setting: Setting, groom: ActorRef, reporter: ActorRef, 
                         tester: ActorRef) 
      extends TaskCounsellor(setting, groom, reporter) {

  override def pushToLaunch(seq: Int, directive: Directive, from: ActorRef) {
    super.pushToLaunch(seq, directive, from)
    val id = directive.task.getId.toString
    LOG.info("When pulling for launching task, id is {} ...", id)
    tester ! id 
  }

  override def pushToResume(seq: Int, directive: Directive, from: ActorRef) {
    super.pushToResume(seq, directive, from)
    val id = directive.task.getId.toString
    LOG.info("When pulling for resuming task, id is {} ...", id)
    tester ! id 
  }

  override def postCancelAck(ack: CancelAck) {
    LOG.debug("<CancelAck> {} receives {}", name, ack)
    tester ! ack.taskAttemptId.toString
  }

} 

object TestExecutor {

  val actorSystemName = "BSPSystem"

  val localhost = Utils.hostname
 
  def content(): String = content(localhost)

  def content(host: String): String = s"""
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
            hostname = "$host" 
            port = 50000
          }
        }
      }
    }
  """

  def config(): Config = Setting.toConfig(content).getConfig("groom")

} 

@RunWith(classOf[JUnitRunner])
class TestExecutor extends TestEnv(TestExecutor.actorSystemName, 
                                   TestExecutor.config) with JobUtil {

  override def beforeAll = System.getProperty("hama.home.dir") match {
    case null => {
      val pwd = System.getProperty("user.dir")
      LOG.info("Configure `hama.home.dir' to {}", pwd)
      System.setProperty("hama.home.dir", pwd)
    }
    case pwd@_ => LOG.info("`hama.home.dir' is configured to {}", pwd)
  }

  override def afterAll { }

  def newDirective(action: Directive.Action, task: Task): Directive = 
    new Directive(action, task, "testMaster")

  def config(conf: HamaConfiguration) {
    //conf.setBoolean("groom.executor.log.console", true)
    //conf.set("groom.actor-system.name", 
             //TestExecutor.actorSystemName) // for MockContainer lookup
    conf.set("bsp.working.dir", testRoot.getCanonicalPath)
    //conf.setClass("container.main", classOf[Container], classOf[Container])
    conf.setBoolean("groom.request.task", false)
  }

  it("test task management, executor, and container ...") {

    val setting = Setting.groom
    config(setting.hama)

    val reporter = createWithArgs("reporter", classOf[Mock])
    val groom = createWithArgs(GroomServer.simpleName(setting), 
      classOf[MockGroom1], setting, reporter, tester) 
 
    /* jobid, taskId, taskAttemptId */
    val task1 = createTask("test", 1, 7, 2) 
    val directive1 = newDirective(Launch, task1)  // launch task
    groom ! directive1
    LOG.info("Task1's id is {}", task1.getId) // attempt_test_0001_000007_2

/*
    val task2 = createTask("test", 3, 1, 3) 
    val directive2 = newDirective(Resume, task2) // resume task
    taskCounsellor ! directive2
    LOG.info("Task2's id is {}", task2.getId) // attempt_test_0003_000001_3
*/

    val waitTime = 30.seconds

    LOG.info("Wait for {} secs ...", waitTime)
    sleep(waitTime)
/*
    expectAnyOf("attempt_test_0001_000007_2", "attempt_test_0003_000001_3")
    expectAnyOf("attempt_test_0001_000007_2", "attempt_test_0003_000001_3")
*/

    LOG.info("Done TestExecutor!")
  }
}
