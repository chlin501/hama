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
package org.apache.hama.sync

import akka.actor.ActorRef
import akka.actor.ActorSystem
import org.apache.hama.HamaConfiguration
import org.apache.hama.Agent
import org.apache.hama.Tester
import org.apache.hama.TestEnv
import org.apache.hama.zk.LocalZooKeeper
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

final case class Config(jobId: String, superstep: Long, totalTasks: Int)
final case class Join(peer: ActorRef)
final case class Start(delay: FiniteDuration = 0.seconds)
final case class Message(content: String)
final case object GetResult
final case class Result(inEnterFun: Boolean, afterLeaveFun: Boolean, 
                        actorName: String)

class MockBarrier(conf: HamaConfiguration, ref: ActorRef) 
    extends Tester(conf, ref) with BarrierParticipant with Agent {

  var inEnterFun: Boolean = false 
  var afterLeaveFun: Boolean = false 
  var actorName: String = _
  var peer: ActorRef = _

  override def log(message: String) = LOG.info(message)

  override def enter = {
    super.enter
    inEnterFun = true
    LOG.info("{} enters barrier? {}.", self.path.name, inEnterFun)
  }

  override def leave = {
    super.leave
    afterLeaveFun = true
    LOG.info("{} leaves barrier? {}", self.path.name, afterLeaveFun)
  }

  def config: Receive = {
    case Config(jobId, superstep, totalTasks) => {
      configure(testConfiguration)
      build(jobId, superstep, totalTasks)
    }
  }

  def start: Receive = {
    case Start(delay) => {
      enter
      if(!0.seconds.equals(delay)) {
        LOG.info("Sleep {} seconds.", delay.toSeconds)
        Thread.sleep(delay.toMillis)
      }
      LOG.info("{} sends messages.", self.path.name)
      peer ! Message(self.path.name)
      leave
    }
  }

  def message: Receive = {
    case Message(n) => {
      actorName = n 
    }
  }

  def join: Receive = {
    case Join(target) => {
      peer = target
      LOG.info("{} joins this test.", peer.path.name)
    }
  }

  def getResult: Receive = {
    case GetResult => {
      tester ! Result(inEnterFun, afterLeaveFun, actorName)
    }
  }

  override def receive = config orElse join orElse start orElse message orElse getResult orElse unknown
  
}

@RunWith(classOf[JUnitRunner])
class TestParticipant extends TestEnv(ActorSystem("TestParticipant")) 
                      with LocalZooKeeper {

  var barrier1: ActorRef = _
  var barrier2: ActorRef = _

  override protected def beforeAll = launchZk

  override protected def afterAll = {
    closeZk
    super.afterAll
  }

  override def log(message: String) = LOG.info(message)

  it("test barrier sync") {
    LOG.info("Test barrier sync.")
    barrier1 = create("barrier1", classOf[MockBarrier])
    barrier2 = create("barrier2", classOf[MockBarrier])

    barrier1 ! Config("test_sync_00001", 7, 2)
    barrier2 ! Config("test_sync_00001", 7, 2)

    barrier1 ! Join(barrier2)
    barrier2 ! Join(barrier1)

    barrier1 ! Start(0.seconds)
    barrier2 ! Start(5.seconds)

    sleep(10.seconds)

    barrier1 ! GetResult
    expect(Result(true, true, "barrier2"))
    barrier2 ! GetResult
    expect(Result(true, true, "barrier1"))
  }
}
