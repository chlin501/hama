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

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.testkit.TestActor
import akka.testkit.TestKit
import akka.testkit.TestProbe
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import java.net.InetAddress
import org.apache.hama.conf.Setting
import org.apache.hama.logging.CommonLog
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSpecLike
import org.scalatest.ShouldMatchers
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

object MultiNodesEnv {

  def parseString(str: String): Config = ConfigFactory.parseString(str)

  def setting(): Setting = new MultiNodesSetting(new HamaConfiguration)

  def setting(conf: HamaConfiguration): Setting = new MultiNodesSetting(conf)
}

class Tester extends Actor { // TODO: with assert functions for verification

  override def receive = {
    case msg@_ => println("unknown message: "+msg)
  }
}

class MultiNodesSetting(conf: HamaConfiguration) extends Setting {

  def hama(): HamaConfiguration = conf

  override def config(): Config = ConfigFactory.parseString(" test { " + 
    akka(host, port, "test") + " }").getConfig("test")

  def info(system: String, host: String, port: Int): SystemInfo = 
    new SystemInfo(system, host, port)

  def info(): SystemInfo = info(sys, host, port)

  def name(): String = conf.get("tester.name", "test")

  def main(): Class[_] = classOf[Tester]

  def sys(): String = conf.get("test.actor-system.name", 
                               "TestSystem")

  def host(): String = conf.get("test.host", 
                                InetAddress.getLocalHost.getHostName)

  def port(): Int = conf.getInt("test.port", 10000)
}

class MultiNodesEnv(actorSystem: ActorSystem) 
      extends TestKit(actorSystem) with FunSpecLike 
                                   with ShouldMatchers 
                                   with BeforeAndAfterAll 
                                   with CommonLog {

  import MultiNodesEnv._

  val probe = TestProbe()

  /**
   * Instantiate test environment with name only.
   * @param name is the actor system name.
   */
  def this(name: String) = this(ActorSystem(name, MultiNodesEnv.setting.config))

  /**
   * Instantiate test environment with name and {@link Config} object.
   * @param name of the actor system.
   * @param config object.
   */
  def this(name: String, conf: HamaConfiguration) = 
    this(ActorSystem(name, MultiNodesEnv.setting(conf).config))

  override protected def afterAll = {
    system.shutdown
  }
  
  /**
   * Create testActor with variable arguments.
   * @param name of the testActor.
   * @param clazz denotes the actual actor class implementation.
   * @param args contains the rest of arguments.
   * @return ActorRef of the target testActor. 
  protected def createWithArgs(name: String, clazz: Class[_], args: Any*): 
    ActorRef = system.actorOf(Props(clazz, args:_*), name)
  
   * Check if a message received is as expected.
   * @param message to be exaimed.
  protected def expect(message: Any) = probe.expectMsg(message)

   * Expect message by waiting 10 seconds.
   * @param msg to be exaimed.
  protected def expect10(msg: Any) = expect(10.seconds, msg)

   * Expect message by waiting up to max seconds
   * @param max duration waiting mesage to be verified.
   * @param msg to be exaimed.
  protected def expect(max: FiniteDuration, message: Any) = 
    probe.expectMsg(max, message)

   * Check if messages is one of provided messages.
   * @param messages that may be returned.
  protected def expectAnyOf(messages: Any*) = probe.expectMsgAnyOf(messages:_*)
   */

  /**
   * Thread sleep {@link FiniteDuration} of time.
   * @param duration with default to 3 seconds.
   */
  protected def sleep(duration: FiniteDuration = 3.seconds) = 
    Thread.sleep(duration.toMillis)

  /**
   * Test actor reference.
   * @return ActorRef of {@link TestProbe#ref}
  protected def tester: ActorRef = probe.ref
   */

}
