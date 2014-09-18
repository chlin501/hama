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
package org.apache.hama.bsp.v2

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.event.Logging
import java.net.InetAddress
import org.apache.hadoop.fs.Path
import org.apache.hama.Agent
import org.apache.hama.groom.Container
import org.apache.hama.message.PeerMessenger
import org.apache.hama.HamaConfiguration
import org.apache.hama.logging.TaskLogger
import org.apache.hama.logging.TaskLogging
import org.apache.hama.TestEnv
import org.apache.hama.util.JobUtil
import org.apache.hama.zk.LocalZooKeeper
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration.DurationInt

final case object Init
final case object GetAllPeers
final case object GetPeerName
final case object GetNumPeers
final case object GetTaskAttemptId
final case object GetSuperstepCount
final case object PeerNameAt
final case object GetPeerIndex
final case object SyncThenValidateSuperstep

class MockWorker(conf: HamaConfiguration, tester: ActorRef, task: Task, 
                 container: ActorRef, peerMessenger: ActorRef, 
                 tasklog: ActorRef)
      extends TaskWorker(conf, container, peerMessenger, tasklog) {

  // getPeerName shouldn't return 0.0.0.0 
  def getPeerName: Receive = {
    case GetPeerName => {
      val currentPeerName = peer.getPeerName 
      tester ! currentPeerName 
    }
  }

  def getAllPeers: Receive = {
    case GetAllPeers => {
      val allPeers = peer.getAllPeerNames
      tester ! allPeers.mkString(",")
    }
  }

  def getNumPeers: Receive = {
    case GetNumPeers => {
      val num = peer.getNumPeers
      tester ! num
    }
  }

  def getTaskAttemptId: Receive = {
    case GetTaskAttemptId => {
      val id = peer.getTaskAttemptId
      tester ! id.toString
    }
  }

  def getSuperstepCount: Receive = {
    case GetSuperstepCount => {
      val superstep = peer.getSuperstepCount
      tester ! superstep
    }
  }

  def peerNameAt: Receive = {
    case PeerNameAt => {
      val seq = conf.getInt("bsp.child.slot.seq", -1)
      if(-1 == seq) throw new RuntimeException("Wrong seq -1 for bsp peer!")
      val name = peer.getPeerName((seq-1))
      LOG.info("Peer name at {} is {}", (seq-1), name)
      tester ! name 
    }
  }

  def getPeerIndex: Receive = {
    case GetPeerIndex => tester ! peer.getPeerIndex 
  }

  def syncThenValidateSuperstep: Receive = {
    case SyncThenValidateSuperstep => {
      LOG.info("Start sync barrier for {} ...", task.getId)
      val start = System.currentTimeMillis
      peer.sync
      val elapsed = System.currentTimeMillis - start
      LOG.info("Finish sync barrier, taking around {} milli secs.", elapsed)
      tester ! peer.getSuperstepCount
    }
  }

  override def receive = getPeerName orElse getAllPeers orElse getNumPeers orElse getTaskAttemptId orElse peerNameAt orElse getSuperstepCount orElse getPeerIndex orElse syncThenValidateSuperstep orElse super.receive orElse unknown
}

@RunWith(classOf[JUnitRunner])
class TestCoordinator extends TestEnv("TestCoordinator") with JobUtil 
                                                         with LocalZooKeeper {

  var conf1 = new HamaConfiguration(testConfiguration)
  var conf2 = new HamaConfiguration(testConfiguration)

  override def beforeAll {
    super.beforeAll
    testConfiguration.setInt("bsp.child.slot.seq", 3)
    conf1 = configure(conf1)
    conf2 = configure(conf2, 2, 2, 62000)
    launchZk
  }

  override protected def afterAll = {
    closeZk
    super.afterAll
  }

  def configure(conf: HamaConfiguration, 
                seq: Int = 1,
                numPeers: Int = 2,
                port: Int = 61000): HamaConfiguration = {
    conf.setInt("bsp.child.slot.seq", seq)
    conf.set("hama.zookeeper.quorum", "localhost:2181")
    conf.setInt("hama.zookeeper.property.clientPort", 2181)
    conf.setInt("bsp.peers.num", numPeers)
    conf.set("bsp.peer.hostname", InetAddress.getLocalHost.getHostName)
    conf.setInt("bsp.peer.port", port) 
    conf
  } 

  def peerName(conf: HamaConfiguration): String = {
    val seq = conf.getInt("bsp.child.slot.seq", 1)
    val ip = conf.get("bsp.peer.hostname", 
                      InetAddress.getLocalHost.getHostName)
    val port = conf.getInt("bsp.peer.port", 61000) 
    val name = "BSPPeerSystem%s@%s:%s".format(seq, ip, port)
    name
  }

  def identifier(conf: HamaConfiguration): String = {
    conf.getInt("bsp.child.slot.seq", -1) match {
      case -1 => throw new RuntimeException("Slot seq shouldn't be -1!")
      case seq@_ => {
        val host = conf.get("bsp.peer.hostname",
                            InetAddress.getLocalHost.getHostName)
        val port = conf.getInt("bsp.peer.port", 61000)
        "BSPPeerSystem%d@%s:%d".format(seq, host, port)
      }
    }
  }

  def slotSeq: Int = testConfiguration.getInt("bsp.child.slot.seq", 3)

  it("test bsp peer coordinator function.") {
     val tasklog = createWithArgs("taskLogger"+slotSeq, classOf[TaskLogger],
                                  testRootPath+"/tasklogs")

    val id = identifier(testConfiguration)
    val peerMessenger = createWithArgs("peerMessenger_"+id, 
                                      classOf[PeerMessenger], testConfiguration)
    val container = createWithArgs("container", classOf[Container],
                                   testConfiguration)
    // job id should be the same, so peers can sync
    val JOB2 = 2 

    val task1 = createTask("test", JOB2, 23, 3)
    assert(null != task1)
    val worker1 = createWithArgs("worker1", classOf[MockWorker], 
                                 conf1, tester, task1, container, peerMessenger,
                                 tasklog)

    val task2 = createTask("test", JOB2, 2, 2)
    assert(null != task2)
    val worker2 = createWithArgs("worker2", classOf[MockWorker], 
                                 conf2, tester, task2, container, peerMessenger,
                                 tasklog)

    worker1 ! ConfigureFor(task1)
    worker2 ! ConfigureFor(task2)

    worker1 ! GetAllPeers
    worker2 ! GetAllPeers
    expectAnyOf(Array(peerName(conf1), peerName(conf2)).mkString(","),
                Array(peerName(conf2), peerName(conf1)).mkString(","))
    expectAnyOf(Array(peerName(conf1), peerName(conf2)).mkString(","),
                Array(peerName(conf2), peerName(conf1)).mkString(","))
    
    worker1 ! GetPeerName
    expect(peerName(conf1)) 
    worker2 ! GetPeerName
    expect(peerName(conf2)) 

    worker1 ! PeerNameAt 
    expectAnyOf(peerName(conf1), peerName(conf2))
    worker2 ! PeerNameAt 
    expectAnyOf(peerName(conf1), peerName(conf2))

    worker1 ! GetNumPeers
    expect(2)
    worker2 ! GetNumPeers
    expect(2)

    worker1 ! GetTaskAttemptId
    expect(task1.getId.toString)
    worker2 ! GetTaskAttemptId
    expect(task2.getId.toString)

    worker1 ! GetSuperstepCount
    expect(2L)
    worker2 ! GetSuperstepCount
    expect(2L)

    worker1 ! GetPeerIndex
    expect(task1.getId.getTaskID.getId)
    worker2 ! GetPeerIndex
    expect(task2.getId.getTaskID.getId)

    // N.B.: for we call sync() so we must allow two coordinators meet at 
    //       that point, otherwise dead lock occurs.
    worker1 ! SyncThenValidateSuperstep
    worker2 ! SyncThenValidateSuperstep
    expect(3L)    
    expect(3L)    

    LOG.info("Done testing Coordinator!")
  }
}
