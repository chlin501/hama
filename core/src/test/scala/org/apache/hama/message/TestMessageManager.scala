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
package org.apache.hama.message

import akka.actor.ActorRef
import java.net.InetAddress
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Writable
import org.apache.hama.HamaConfiguration
import org.apache.hama.TestEnv
import org.apache.hama.util.JobUtil
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class TestMessageManager extends TestEnv("TestMessageManager") with JobUtil {

  val seq = 2
  val host = InetAddress.getLocalHost.getHostName
  val port = 12341
  val expectedPeer = Peer.at("BSPPeerSystem%d@%s:%d".format(seq, host, port))

  override def beforeAll {
    super.beforeAll
    testConfiguration.setInt("bsp.child.slot.seq", seq)
    testConfiguration.set("bsp.peer.hostname", host)
    testConfiguration.setInt("bsp.peer.port", port)
  }

  def identifier(conf: HamaConfiguration): String = {
    conf.getInt("bsp.child.slot.seq", -1) match {
      case -1 => throw new RuntimeException("Slot seq is -1!")
      case seq@_ => {
        val host = conf.get("bsp.peer.hostname",
                            InetAddress.getLocalHost.getHostName)
        val port = conf.getInt("bsp.peer.port", 61000)
        "BSPPeerSystem%d@%s:%d".format(seq, host, port)
      }
    }
  }


  it("test message manager.") {
    val jobId = createJobId("test", 2)
    val taskAttemptId = createTaskAttemptId(jobId, 3, 2)
    val id = identifier(testConfiguration)
    val peerMessenger = createWithArgs("peerMessenger_"+id, 
                                       classOf[PeerMessenger])
    val messageManager = 
      MessageManager.get[IntWritable](testConfiguration, peerMessenger)
    assert(null != messageManager)
    messageManager.init(testConfiguration, taskAttemptId)

    // transfer() -> loopbackmessage() -> messenger.clearoutgoingmessage()
    // so we can verify getCurrentMessage, etc. functions.  

    val numMsgs = messageManager.getNumCurrentMessages 
    LOG.info("Expect 0 messages. Actual "+numMsgs+" messages.")
    assert(0 == numMsgs)
    val currentMsg = messageManager.getCurrentMessage
    LOG.info("Expected null message. Actual message is "+currentMsg)
    assert(null == currentMsg)
    messageManager.loopBackMessage(new IntWritable(78)) 
    messageManager.loopBackMessage(new IntWritable(87)) 
    messageManager.clearOutgoingMessages // move msgs to localQueue
    val msgNum = messageManager.getNumCurrentMessages
    LOG.info("Expected 2 messages. Actual "+msgNum+" received!")
    assert(2 == msgNum)
    val msgCurrent1 = messageManager.getCurrentMessage
    LOG.info("First current message "+msgCurrent1)
    assert(78 == msgCurrent1.get)
    val msgCurrent2 = messageManager.getCurrentMessage
    LOG.info("Second current message "+msgCurrent2)
    assert(87 == msgCurrent2.get)

    // test the rest functions such as peer address, etc.
    val peer = messageManager.getListenerAddress   
    LOG.info("Peer at the machine: "+peer)
    assert(null != peer)
    assert(expectedPeer.equals(peer))
    messageManager.send("BSPPeerSystem2@host31:1294", new IntWritable(7)) 
    messageManager.send("BSPPeerSystem1@server1:2941", new IntWritable(2)) 
    messageManager.send("BSPPeerSystem2@host31:1294", new IntWritable(9)) 
    val itor = messageManager.getOutgoingBundles
    assert(null != itor)
    var peerCnt = 0
    asScalaIterator(itor).foreach( entry => {
      val key = entry.getKey
      val value = entry.getValue
      LOG.info("Found key: "+key+" value: "+value)
      val p = key.getPath
      assert(p.equals(Peer.at("BSPPeerSystem2@host31:1294").getPath) ||
             p.equals(Peer.at("BSPPeerSystem1@server1:2941").getPath))
      val msgItor = value.iterator
      assert(null != msgItor)
      var msgCnt = 0
      asScalaIterator(msgItor).foreach( msg => {
        assert(null != msg)
        LOG.info("Message found with value "+msg.get) 
        assert(7 == msg.get || 2 == msg.get || 9 == msg.get)
        msgCnt += 1
      })
      LOG.info("Expect either 1 or 2 messages. "+msgCnt+" messages found.")
      assert(2 == msgCnt || 1 == msgCnt)
      peerCnt += 1
    })
    LOG.info("Expect 2 peers. "+peerCnt+" peers found.")
    assert(2 == peerCnt)
    messageManager.close       
    val totalMsgs = messageManager.getNumCurrentMessages
    LOG.info("Expected 0 messages. Actual "+totalMsgs+" messages found.")
    assert(0 == totalMsgs)
    LOG.info("Done testing message manager!")

  }
}
