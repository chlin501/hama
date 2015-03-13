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
import org.apache.hama.logging.CommonLog
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSpecLike
import org.scalatest.ShouldMatchers
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

class Mock extends Agent {

  override def receive = close orElse unknown 

}

class MockClient extends Mock 

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

  import TestEnv._

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
    val tmp = new File(testRootPath)
    if(!tmp.exists) tmp.mkdirs
    tmp
  }

  def testHadoop: File = { // TODO: merge testRoot, testHadoop, and testBSP
    val tmp = new File("/tmp/hadoop")
    if(!tmp.exists) tmp.mkdirs
    tmp
  }

  def testBSP: File = {
    val tmp = new File("/tmp/bsp")
    if(!tmp.exists) tmp.mkdirs
    tmp
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
    testHadoop.exists match {
      case true => {
        LOG.info("Delete test hadoop path: "+testHadoop.getCanonicalPath)
        FileUtils.deleteDirectory(testHadoop)
      }
      case false =>
    } 
    testBSP.exists match {
      case true => {
        LOG.info("Delete test bsp path: "+testBSP.getCanonicalPath)
        FileUtils.deleteDirectory(testBSP)
      }
      case false =>
    }
  }

  def mkTestRoot() = testRoot

  def mkdirs(dir: String) = {
    val folder = new File(dir) 
    if(!folder.exists) folder.mkdirs else true
  }

  def constitute(dirs: String*): String = {
    var file = new File(dirs(0))
    dirs.drop(1).foreach ( dir => file = new File(file, dir))
    file.getPath
  }

  override protected def beforeAll = mkTestRoot

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

  protected def createWithDispatcher(name: String, clazz: Class[_], 
                                     disp: String, args: Any*): ActorRef = 
    system.actorOf(Props(clazz, args:_*).withDispatcher(disp), 
                              name)

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
   * Expect message by waiting 10 seconds.
   * @param msg to be exaimed.
   */
  protected def expect10(msg: Any) = expect(10.seconds, msg)

  /**
   * Expect message by waiting up to max seconds
   * @param max duration waiting mesage to be verified.
   * @param msg to be exaimed.
   */
  protected def expect(max: FiniteDuration, message: Any) = 
    probe.expectMsg(max, message)

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

}
