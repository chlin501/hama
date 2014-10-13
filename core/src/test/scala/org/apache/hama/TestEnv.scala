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
package org.apache.hama

import akka.actor.ActorSystem
import akka.actor.ActorRef
import akka.actor.Props
import akka.testkit.TestKit
import akka.testkit.TestProbe
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import java.io.File
import org.apache.commons.io.FileUtils
import org.apache.hadoop.io.Writable
import org.apache.hama.bsp.TaskAttemptID
import org.apache.hama.bsp.v2.Coordinator
import org.apache.hama.bsp.v2.Task
import org.apache.hama.groom.Container
import org.apache.hama.logging.CommonLog
import org.apache.hama.logging.TaskLogger
import org.apache.hama.message.MessageExecutive
import org.apache.hama.sync.BarrierClient
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSpecLike
import org.scalatest.ShouldMatchers
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

object TestEnv {

  /**
   * Used to parse config string to {@link Config}.
   * @param str is config content.
   * @return Config object.
   */
  def parseString(str: String): Config = ConfigFactory.parseString(str)
}

class TestEnv(actorSystem: ActorSystem) extends TestKit(actorSystem) 
                                           with FunSpecLike 
                                           with ShouldMatchers 
                                           with BeforeAndAfterAll 
                                           with CommonLog {


  val probe = TestProbe()
  val conf = new HamaConfiguration()
  val testRootPath = "/tmp/hama"

  /**
   * Instantiate test environment with name only.
   * @param name is the actor system name.
   */
  def this(name: String) = this(ActorSystem(name))

  /**
   * Instantiate test environment with name and {@link Config} object.
   * @param name of the actor system.
   * @param config object.
   */
  def this(name: String, config: Config) = this(ActorSystem(name, config))

  /**
   * This creates a folder for testing.
   * All files within this folder 
   * @return File points to the test root path "/tmp/hama".
   */
  def testRoot: File = {
    val tmpRoot = new File(testRootPath)
    if(!tmpRoot.exists) tmpRoot.mkdirs
    tmpRoot
  }

  /**
   * Delete test root path if the path exists.
   */
  def deleteTestRoot {
    testRoot.exists match {
      case true => {
        LOG.info("Delete test root path: "+testRoot.getCanonicalPath)
        FileUtils.deleteDirectory(testRoot)
      }
      case false =>
    }
  }

  override protected def afterAll = {
    deleteTestRoot
    system.shutdown
  }

  /**
   * Test configuration.
   * @return HamaConfiguration contains particular setting for this
   */
  protected def testConfiguration: HamaConfiguration = conf

  /**
   * Create an test actor reference embedded with configuration and Probe.ref.
   * @param name of the test actor to be created.
   * @param clazz denotes the class object of the test actor.
   * @return ActorRef corresponds to class object passed in.
   */
  protected def createWithTester(name: String, clazz: Class[_]): ActorRef = 
    system.actorOf(Props(clazz, testConfiguration, tester), name)

  /**
   * Create testActor with variable arguments.
   * @param name of the testActor.
   * @param clazz denotes the actual actor class implementation.
   * @param args contains the rest of arguments.
   * @return ActorRef of the target testActor. 
   */
  protected def createWithArgs(name: String, clazz: Class[_], args: Any*): 
    ActorRef = system.actorOf(Props(clazz, args:_*), name)

  
  /**
   * Create testActor without any arguments supplied.
   * @param name of the testActor.
   * @param clazz denotes the actual actor class implementation.
   * @return ActorRef of the target testActor. 
   */  
  protected def createWithoutArgs(name: String, clazz: Class[_]): ActorRef =
    system.actorOf(Props(clazz), name)

  /**
   * Create testActor with testConfiguration.
   * @param name of the testActor.
   * @param clazz denotes the actual actor class implementation.
   * @return ActorRef of the target testActor. 
   */
  protected def create(name: String, clazz: Class[_]): ActorRef = 
    system.actorOf(Props(clazz, testConfiguration), name)

  /**
   * Check if a message received is as expected.
   * @param message to be exaimed.
   */
  protected def expect(message: Any) = probe.expectMsg(message)

  /**
   * Check if messages is one of provided messages.
   * @param messages that may be returned.
   */
  protected def expectAnyOf(messages: Any*) = probe.expectMsgAnyOf(messages:_*)

  /**
   * Thread sleep {@link FiniteDuration} of time.
   * @param duration with default to 3 seconds.
   */
  protected def sleep(duration: FiniteDuration = 3.seconds) = 
    Thread.sleep(duration.toMillis)

  /**
   * Test actor reference.
   * @return ActorRef of {@link TestProbe#ref}
   */
  protected def tester: ActorRef = probe.ref

  // task actor related functions

  def createContainer(): ActorRef =
    createContainer("container", classOf[Container], testConfiguration)

  def createContainer[C <: Container](container: Class[C]): ActorRef =
    createContainer("container", container, testConfiguration)

  def createContainer[C <: Container](name: String,
                                      container: Class[C],
                                      conf: HamaConfiguration): ActorRef =
    createWithArgs(name, container, conf)

  def createTasklog(taskAttemptId: TaskAttemptID, name: String = "tasklog",
                    logDir: String = "/tmp/hama/log", 
                    console: Boolean = true): ActorRef = 
    createWithArgs(name, classOf[TaskLogger], logDir, taskAttemptId, console)

  def createSyncClient[B <: BarrierClient](name: String,
                                           barrier: Class[B],
                                           conf: HamaConfiguration,
                                           taskAttemptId: TaskAttemptID,
                                           tasklog: ActorRef): ActorRef = {
    val client = BarrierClient.get(conf, taskAttemptId)
    createWithArgs(name, barrier, conf, taskAttemptId, client, tasklog, tester)
  }

  def createMessenger[M <: MessageExecutive[Writable]](
      name: String, messenger: Class[M], conf: HamaConfiguration,
      slotSeq: Int, taskAttemptId: TaskAttemptID, container: ActorRef, 
      tasklog: ActorRef, proxy: ProxyInfo): ActorRef = 
    createWithArgs(name, messenger, conf, slotSeq, taskAttemptId, container, 
                   tasklog, proxy, tester)

  def createMessenger[M <: MessageExecutive[Writable]](
      slotSeq: Int, messenger: Class[M], conf: HamaConfiguration,
      taskAttemptId: TaskAttemptID, container: ActorRef, 
      tasklog: ActorRef, proxy: ProxyInfo): ActorRef = {
    val name = "messenger-BSPPeerSystem%s".format(slotSeq)
    createMessenger(name, messenger, conf, slotSeq, taskAttemptId, container, 
                    tasklog, proxy)
  }

  def createCoordinator[C <: Coordinator](name: String,
                                          coordinator: Class[C], 
                                          conf: HamaConfiguration,
                                          task: Task, 
                                          container: ActorRef, 
                                          messenger: ActorRef,
                                          syncClient: ActorRef, 
                                          tasklog: ActorRef): ActorRef = 
    createWithArgs(name, coordinator, conf, task, container, messenger, 
                   syncClient, tasklog, tester)
  
}
