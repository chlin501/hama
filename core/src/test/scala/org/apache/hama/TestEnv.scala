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
import org.apache.hama.util.Logger
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSpecLike
import org.scalatest.ShouldMatchers
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

class TestEnv(actorSystem: ActorSystem) extends TestKit(actorSystem) 
                                           with FunSpecLike 
                                           with ShouldMatchers 
                                           with BeforeAndAfterAll 
                                           with Logger {

  val probe = TestProbe()
  val conf = new HamaConfiguration()

  override protected def afterAll = system.shutdown

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
  protected def createTester(name: String, clazz: Class[_]): ActorRef = 
    system.actorOf(Props(clazz, testConfiguration, tester), name)

  protected def createWithArgs(name: String, clazz: Class[_], args: Any*): 
    ActorRef = system.actorOf(Props(clazz, args:_*), name)

  protected def create(name: String, clazz: Class[_]): 
    ActorRef = system.actorOf(Props(clazz, testConfiguration), name)

  /**
   * Check if a message received is as expected.
   * @param message to be exaimed.
   */
  protected def expect(message: Any) = probe.expectMsg(message)

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
