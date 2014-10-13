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

class MockBarrierClient(conf: HamaConfiguration, taskAttemptId: TaskAttemptID, 
                        client: PeerSyncClient, tasklog: ActorRef, 
                        tester: ActorRef)
      extends BarrierClient(conf, taskAttemptId, client, tasklog) {

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
class TestBarrierClient extends TestEnv("TestBarrierClient") 
                        with LocalZooKeeper 
                        with JobUtil {

  val host = InetAddress.getLocalHost.getHostName

  val sys1 = "BSPPeerSystem1@%s:61000".format(host)
  val sys2 = "BSPPeerSystem1@%s:61100".format(host)

  val conf1 = config(host)
  val conf2 = config(host, port = 61100)

  override def beforeAll {
    super.beforeAll
    launchZk
  }
 
  override def afterAll {
    closeZk
    super.afterAll 
  }

  def createTaskLog(name: String, hamaHomePath: String, 
                    taskAttemptId: TaskAttemptID, 
                    console: Boolean = true): ActorRef = 
    createWithArgs(name, classOf[TaskLogger], hamaHomePath, taskAttemptId,
                   console)

  def createClient(name: String, conf: HamaConfiguration, 
                   taskAttemptId: TaskAttemptID, tasklog: ActorRef): 
      ActorRef = {
    val client = BarrierClient.get(conf, taskAttemptId)
    createWithArgs(name, classOf[MockBarrierClient], conf, taskAttemptId, 
                   client, tasklog, tester)
  }

  it("test barrier client.") {
    var superstep = 0
    val taskId1 = createTaskAttemptId("test", 1, 1, 1)
    val taskId2 = createTaskAttemptId("test", 1, 2, 1)

    val tasklog = createTaskLog("tasklog", testRootPath, taskId1)

    val client1 = createClient("client1", conf1, taskId1, tasklog)
    val client2 = createClient("client2", conf2, taskId2, tasklog)

    //client1 ! SetTaskAttemptId(taskId1)
    //client2 ! SetTaskAttemptId(taskId2)

    LOG.info("'Enter' barrier ...")
    client1 ! Enter(superstep)
    client2 ! Enter(superstep)
    expect(WithinBarrier)
    expect(WithinBarrier)

    //superstep += 1
    //LOG.info("Superstep value, expected 1, is {}", superstep)
    //assert(superstep == 1)

    LOG.info("'Leave' barrier ...")
    client1 ! Leave(superstep)
    client2 ! Leave(superstep)
    expect(ExitBarrier)
    expect(ExitBarrier)

    val peer1 = Utils.await[PeerName](client1, GetPeerName) 
    LOG.info("Actual peer1's name is {}, expected {}", peer1.peerName, sys1)
    assert(sys1.equals(peer1.peerName))

    val peer2 = Utils.await[PeerName](client2, GetPeerName)
    LOG.info("Actual peer2's name is {}, expected {}", peer2.peerName, sys2)
    assert(sys2.equals(peer2.peerName))

    val peerAtIdx1 = Utils.await[PeerNameByIndex](client1, GetPeerNameBy(1))
    LOG.info("Peer at index 1 value is {}", peerAtIdx1.name)
    assert(sys2.equals(peerAtIdx1.name))
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
