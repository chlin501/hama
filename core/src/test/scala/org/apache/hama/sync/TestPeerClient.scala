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
import java.net.InetAddress
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.Writable
import org.apache.hama.HamaConfiguration
import org.apache.hama.TestEnv
import org.apache.hama.bsp.TaskAttemptID
import org.apache.hama.logging.TaskLogger
import org.apache.hama.util.JobUtil
import org.apache.hama.util.ZkUtil._
import org.apache.hama.util.Utils
import org.apache.hama.zk.LocalZooKeeper
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.collection.JavaConversions._

class MockPeerClient(conf: HamaConfiguration,
                     taskAttemptId: TaskAttemptID, 
                     syncer: Barrier,
                     operator: PeerRegistrator, 
                     tasklog: ActorRef,
                     tester: ActorRef)
      extends PeerClient(conf, taskAttemptId, syncer, operator, tasklog) {

  override def withinBarrier(from: ActorRef) = {
    println("Notify "+from+" now is WithinBerrirer!")
    tester ! WithinBarrier
  }

  override def exitBarrier(from: ActorRef) = {
    println("Notify "+from+" now is ExitBerrirer!")
    tester ! ExitBarrier
  }

}

@RunWith(classOf[JUnitRunner])
class TestPeerClient extends TestEnv("TestPeerClient") 
                        with LocalZooKeeper 
                        with JobUtil {

  val host = InetAddress.getLocalHost.getHostName

  val slotSeq = 1

  val sys1 = "BSPPeerSystem%s@%s:61000".format(slotSeq, host)
  val sys2 = "BSPPeerSystem%s@%s:61100".format(slotSeq, host)

  // common conf for client 1
  val conf1 = config(host)
  // common conf for client 2
  val conf2 = config(host, port = 61100)

  val superstep = 0
  val numBSPTasks = 2

  override def beforeAll {
    super.beforeAll
    launchZk
  }
 
  override def afterAll {
    closeZk
    super.afterAll 
  }

  it("test peer client that holds barrier sync and name registrator.") {
    val taskId1 = createTaskAttemptId("test", 1, 1, 1)
    val taskId2 = createTaskAttemptId("test", 1, 2, 1)

    val tasklog = createWithArgs("tasklog", classOf[TaskLogger], "/tmp/hama/log", taskId1, true)

    val syncer1 = CuratorBarrier(conf1, taskId1, numBSPTasks) 
    val operator1 = CuratorRegistrator(conf1) 
    val client1 = createWithArgs("client1", classOf[MockPeerClient], conf1, taskId1, syncer1, operator1, tasklog, tester)

    val syncer2 = CuratorBarrier(conf2, taskId2, numBSPTasks) 
    val operator2 = CuratorRegistrator(conf2) 
    val client2 = createWithArgs("client2", classOf[MockPeerClient], conf2, taskId2, syncer2, operator2, tasklog, tester)

    LOG.info("Peer registers itself to ZooKeeper ...")
    client1 ! Register
    client2 ! Register

    LOG.info("Peer 'Enter' barrier ...")
    client1 ! Enter(superstep)
    client2 ! Enter(superstep)
    expect(WithinBarrier)
    expect(WithinBarrier)

    LOG.info("Peer 'Leave' barrier ...")
    client1 ! Leave(superstep)
    client2 ! Leave(superstep)
    expect(ExitBarrier)
    expect(ExitBarrier)

    LOG.info("Synchronous operations for obtaining peer information ...")
    val peer1 = Utils.await[PeerName](client1, GetPeerName) 
    LOG.info("Actual peer1's name is {}, expected {}", peer1.peerName, sys1)
    assert(sys1.equals(peer1.peerName))

    val peer2 = Utils.await[PeerName](client2, GetPeerName)
    LOG.info("Actual peer2's name is {}, expected {}", peer2.peerName, sys2)
    assert(sys2.equals(peer2.peerName))

    LOG.info("Find peer name at index 1 ...")
    val peerAtIdx1 = Utils.await[PeerNameByIndex](client1, GetPeerNameBy(1))
    LOG.info("Peer at index 1 value is {}", peerAtIdx1.name)
    assert(sys2.equals(peerAtIdx1.name))
    LOG.info("Find peer name at index 0 ...")
    val peerAtIdx0 = Utils.await[PeerNameByIndex](client2, GetPeerNameBy(0))
    LOG.info("Peer at index 0 value is {}", peerAtIdx0.name)
    assert(sys1.equals(peerAtIdx0.name))

    val foundLength1 = Utils.await[NumPeers](client1, GetNumPeers)
    val foundLength2 = Utils.await[NumPeers](client2, GetNumPeers)
    LOG.info("Peer length found by client 1 is `{}', client 2 `{}'", 
             foundLength1.num, foundLength2.num)
    assert(foundLength1.num == foundLength2.num)

    LOG.info("Done testing barrier client!")  
  }
}

