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
import org.apache.hama.ProxyInfo
import org.apache.hama.TestEnv
import org.apache.hama.logging.TaskLogger
import org.apache.hama.message.compress.BSPMessageCompressor
import org.apache.hama.util.JobUtil
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.collection.JavaConversions._
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration.DurationInt

final case object GetSentMessage

class MockMessageExecutive[M <: Writable](conf: HamaConfiguration,
                                          slotSeq: Int,
                                          taskAttemptId: TaskAttemptID,
                                          tasklog: ActorRef,
                                          tester: ActorRef,
                                          target: ProxyInfo)
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

  override def lookupPeer(name: String, addr: String) = {
    val containerRemoved = addr.replaceAll("container/", "")
    println("remove container within actor path: "+containerRemoved)
    val sysName = target.getActorSystemName
    val replaced = containerRemoved.replaceFirst(sysName, context.system.name)
    println("replace first system address ("+sysName+") with ("+
            context.system.name+") => " +replaced)
    lookup(name, replaced)
  }

  override def proxyOf(target: String, ref: ActorRef,
                       retryAfter: FiniteDuration = 100.millis): ActorRef = ref 

  override def putToLocal(bundle: BSPMessageBundle[M]) {
    val beforeSize = localQueue.size
    println("actor name: "+name+" [before] local queue's size is "+beforeSize) 
    super.putToLocal(bundle)
    val afterSize = localQueue.size
    println("actor name: "+name+" [after] local queue's size is "+afterSize) 
    tester ! afterSize
  } 

  override def currentMessage: Receive = {
    case GetCurrentMessage => {
      val msg = getCurrentMessage
      println("Retrieve "+name+"'s current msg => "+msg)
      tester ! msg.toString
    }
  }

  override def listenerAddress: Receive = {
    case GetListenerAddress => {
      val currentPeer = getListenerAddress
      println("actor "+name+" has current peer address "+currentPeer)
      tester ! currentPeer
    }
  }

  override def receive = getSentMessage orElse super.receive
}

object TestMessageExecutive {

  val sysName = "TestMessageExecutive"

}

@RunWith(classOf[JUnitRunner])
class TestMessageExecutive extends TestEnv(TestMessageExecutive.sysName) 
                           with JobUtil {

  import TestMessageExecutive._

  val slotSeq1 = 1
  val slotSeq2 = 2
  val bspPeerSystem1 = "BSPPeerSystem%s".format(slotSeq1)
  val bspPeerSystem2 = "BSPPeerSystem%s".format(slotSeq2)
  val logDir = testRootPath+"/messeage"

  val host = InetAddress.getLocalHost.getHostName
  val port = 61000 

  def currentPeer(slotSeq: Int): ProxyInfo = {
    val sys = "BSPPeerSystem%s@%s:%s".format(slotSeq, host, port)
    Peer.at(sys)
  }

  def createBundle[M <: Writable](msgs: M*): BSPMessageBundle[M] = {
    val bundle = new BSPMessageBundle[M]()
    bundle.setCompressor(
      BSPMessageCompressor.get(testConfiguration), 
      BSPMessageCompressor.threshold(Option(testConfiguration))
    )
    msgs.foreach( msg => bundle.addMessage(msg))
    bundle
  }

  def createTaskLogger(slotSeq: Int, taskAttemptId: TaskAttemptID,
                       console: Boolean = true): ActorRef = {
    val tasklog = createWithArgs("taskLogger%s".format(slotSeq), 
                                 classOf[TaskLogger], 
                                 logDir, 
                                 taskAttemptId, 
                                 console)
    assert(null != tasklog)
    tasklog
  }

  def createMsgMgr(slotSeq: Int, taskAttemptId: TaskAttemptID, 
                   tasklog: ActorRef, proxy: ProxyInfo): ActorRef = {
    val messenger = createWithArgs("messenger_BSPPeerSystem%s".format(slotSeq), 
                                   classOf[MockMessageExecutive[Writable]], 
                                   testConfiguration, 
                                   slotSeq, 
                                   taskAttemptId, 
                                   tasklog,
                                   tester, 
                                   proxy)
    assert(null != messenger)

    messenger
  }

  it("test message executive functions.") {
    val jobId = createJobId("test", 2)
    val taskAttemptId1 = createTaskAttemptId(jobId, 3, 4)
    val taskAttemptId2 = createTaskAttemptId(jobId, 4, 2)
  
    // log dir will both create at logDir/jobId so we only create 1 task logger
    val tasklog = createTaskLogger(slotSeq1, taskAttemptId1) 

    val proxy1 = Peer.at(bspPeerSystem1) 
    val proxy2 = Peer.at(bspPeerSystem2) 
    LOG.info("proxy1 is at{}. proxy2 is at {}", proxy1, proxy2)

    val messenger1 = createMsgMgr(slotSeq1, taskAttemptId1, tasklog, proxy2)
    val messenger2 = createMsgMgr(slotSeq2, taskAttemptId2, tasklog, proxy1)

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


    LOG.info("test send method ...")
    messenger1 ! Send(proxy2.getAddress, bundle2a)
    messenger1 ! Send(proxy2.getAddress, bundle2b)
    messenger2 ! Send(proxy1.getAddress, bundle1)

    messenger1 ! GetSentMessage
    expect((proxy2.getPath, seq2a))
    expect((proxy2.getPath, seq2b))
    messenger2 ! GetSentMessage
    expect((proxy1.getPath, seq1))

    messenger1 ! Transfer(proxy2, bundle2a)
    expect(3)
    messenger1 ! Transfer(proxy2, bundle2b)
    expect(6)
    messenger2 ! GetCurrentMessage
    expectAnyOf("mem: 4096", "cpu: 486", "disk: 512")
    messenger2 ! GetCurrentMessage
    expectAnyOf("mem: 4096", "cpu: 486", "disk: 512")
    messenger2 ! GetCurrentMessage
    expectAnyOf("mem: 4096", "cpu: 486", "disk: 512")

    messenger2 ! GetCurrentMessage
    expectAnyOf("2135", "124", "22111")
    messenger2 ! GetCurrentMessage
    expectAnyOf("2135", "124", "22111")
    messenger2 ! GetCurrentMessage
    expectAnyOf("2135", "124", "22111")
    messenger1 ! GetListenerAddress
    expect(currentPeer(slotSeq1))

    messenger2 ! Transfer(proxy1, bundle1)
    expect(3)
    messenger1 ! GetCurrentMessage
    expectAnyOf("3", "6", "9")
    messenger1 ! GetCurrentMessage
    expectAnyOf("3", "6", "9")
    messenger1 ! GetCurrentMessage
    expectAnyOf("3", "6", "9")
    messenger2 ! GetListenerAddress
    expect(currentPeer(slotSeq2))

    LOG.info("Done testing message manager!")

  }
}

