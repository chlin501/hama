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
import java.io.IOException
import java.net.InetAddress
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.Writable
import org.apache.hama.bsp.TaskAttemptID
import org.apache.hama.HamaConfiguration
import org.apache.hama.TestEnv
import org.apache.hama.logging.TaskLogger
import org.apache.hama.message.compress.BSPMessageCompressor
import org.apache.hama.util.JobUtil
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.collection.JavaConversions._

final case object GetSentMessage

class MockMessageExecutive[M <: Writable](conf: HamaConfiguration,
                                          slotSeq: Int,
                                          taskAttemptId: TaskAttemptID,
                                          tasklog: ActorRef,
                                          tester: ActorRef) 
      extends MessageExecutive[M](conf, slotSeq, taskAttemptId, tasklog) {

  def getSentMessage: Receive = {
    case GetSentMessage => {
      asScalaIterator(getOutgoingBundles).foreach ( entry => {
        val proxy = entry.getKey
        val bundle = entry.getValue
        asScalaIterator(bundle.iterator).foreach ( bundle1 => {
          val it = bundle1.asInstanceOf[BSPMessageBundle[M]].iterator
          var seq = Seq[String]()
          asScalaIterator(it).foreach ( msg =>  seq ++= Seq(msg.toString) )
          println("Target actor: "+proxy.getPath+", collected seq: "+seq)
          tester ! (proxy.getPath, seq)
        })
      })
    }
  } 
  
   
  override def receive = getSentMessage orElse super.receive
}


@RunWith(classOf[JUnitRunner])
class TestMessageExecutive extends TestEnv("TestMessageExecutive") 
                           with JobUtil {
  val sysName = "TestMessageExecutive"
  val slotSeq1 = 1
  val slotSeq2 = 2
  val bspPeerSystem1 = "BSPPeerSystem%s".format(slotSeq1)
  val bspPeerSystem2 = "BSPPeerSystem%s".format(slotSeq2)
  val logDir = testRootPath+"/messeage"

  val host = InetAddress.getLocalHost.getHostName
  val port = 12341


  def createBundle[M <: Writable](msgs: M*): BSPMessageBundle[M] = {
    val bundle = new BSPMessageBundle[M]()
    bundle.setCompressor(
      BSPMessageCompressor.get(testConfiguration), 
      BSPMessageCompressor.threshold(Option(testConfiguration))
    )
    msgs.foreach( msg => bundle.addMessage(msg))
    bundle
  }

  def createTaskLogger(slotSeq: Int, taskAttemptId: TaskAttemptID): ActorRef = {
    val tasklog = createWithArgs("taskLogger%s".format(slotSeq), 
                             classOf[TaskLogger], logDir, taskAttemptId)
    assert(null != tasklog)
    tasklog
  }

  def createMsgMgr(slotSeq: Int, taskAttemptId: TaskAttemptID, 
                   tasklog: ActorRef): ActorRef = {
    val messenger = createWithArgs("messenger_BSPPeerSystem%s".format(slotSeq), 
                                   classOf[MockMessageExecutive[Writable]], 
                                   testConfiguration, 
                                   slotSeq, 
                                   taskAttemptId, 
                                   tasklog,
                                   tester)
    assert(null != messenger)

    messenger
  }

  override def afterAll =  system.shutdown  // temp disable delete /tmp/hama

  it("test message executive functions.") {
    val jobId = createJobId("test", 2)
    val taskAttemptId1 = createTaskAttemptId(jobId, 3, 4)
    val taskAttemptId2 = createTaskAttemptId(jobId, 4, 2)
  
    // log dir will both create at logDir/jobId so we only create 1 task logger
    val tasklog = createTaskLogger(slotSeq1, taskAttemptId1) 

    val messenger1 = createMsgMgr(slotSeq1, taskAttemptId1, tasklog)
    val messenger2 = createMsgMgr(slotSeq2, taskAttemptId2, tasklog)

    val bundle1 = createBundle[IntWritable](new IntWritable(3), 
                                            new IntWritable(6), 
                                            new IntWritable(9)) 
    val seq1 = Seq[String]("3", "6", "9")

    val bundle2a = createBundle[Text](new Text("mem: 4096"), 
                                      new Text("cpu: 486"), 
                                      new Text("disk: 512")) 
    val seq2a = Seq[String]("mem: 4096", "cpu: 486", "disk: 512")
    val bundle2b = createBundle[LongWritable](new LongWritable(2135), 
                                              new LongWritable(124), 
                                              new LongWritable(22111)) 
    val seq2b = Seq[String]("2135", "124", "22111")

    val proxy1 = Peer.at(bspPeerSystem1) 
    val proxy2 = Peer.at(bspPeerSystem2) 

    LOG.info("proxy1 is at{}. proxy2 is at {}", proxy1, proxy2)

    LOG.info("test send method ...")
    messenger1 ! Send(proxy2.getAddress, bundle2a)
    messenger1 ! Send(proxy2.getAddress, bundle2b)
    messenger2 ! Send(proxy1.getAddress, bundle1)

    messenger1 ! GetSentMessage
    expect((proxy2.getPath, seq2a))
    expect((proxy2.getPath, seq2b))
    messenger2 ! GetSentMessage
    expect((proxy1.getPath, seq1))


/*
    messenger1 ! GetOutgoingBundles
   

    messenger1 ! Transfer(proxy2, bundle2a)
    messenger1 ! Transfer(proxy2, bundle2b)
    messenger2 ! Transfer(proxy1, bundle1)
*/


/*

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
*/
    LOG.info("Done testing message manager!")

  }
}

