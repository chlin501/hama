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
package org.apache.hama.monitor

import akka.actor.ActorRef
import java.io.DataOutputStream
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.BooleanWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.MapWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.Writable
import org.apache.hama.bsp.v2.Superstep
import org.apache.hama.HamaConfiguration
import org.apache.hama.message.BSPMessageBundle
import org.apache.hama.message.CombinerUtil
import org.apache.hama.message.compress.BSPMessageCompressor
//import org.apache.hama.message.OutgoingMessageManager
import org.apache.hama.message.Peer
import org.apache.hama.ProxyInfo
import org.apache.hama.TestEnv
import org.apache.hama.util.JobUtil
import org.apache.hama.zk.LocalZooKeeper
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.collection.JavaConversions._

/*
final case class PeerAndMessages[M <: Writable](ckptPath: String, 
                                                peer: ProxyInfo,
                                                msg: M)
final case class NameAndMap(ckptPath: String, className: String, 
                            variables: Map[String, String])
*/

final case class Msgs(arg1: Int, arg2: Int, arg3: Int)

object MockCheckpointer {

  val tmpRootPath = "/tmp/hama/bsp/ckpt"

}

class MockCheckpointer(commConf: HamaConfiguration,
                       taskConf: HamaConfiguration,
                       taskAttemptId: String,
                       superstep: Long,
                       tester: ActorRef) 
      extends Checkpointer(commConf, taskConf, taskAttemptId, superstep) {

  import MockCheckpointer._

  override def getRootPath(taskConf: HamaConfiguration): String = {
    val path = taskConf.get("bsp.checkpoint.root.path", tmpRootPath) 
    LOG.info("Checkpoint file will be writtent to {} directory.", path)
    path 
  }

  override def toMapWritable(variables: Map[String, Writable]): MapWritable = {
    val mapWritable = super.toMapWritable(variables)
    LOG.info("MapWritable to be verified -> {}", mapWritable)
    if(1 != mapWritable.size) 
      throw new RuntimeException("Map variables size is not 1!")
    val count = mapWritable.get(new Text("superstepCount"))
    tester ! count.asInstanceOf[LongWritable].get
    mapWritable
  }  

  override def writeText(next: Class[_ <: Superstep])(out: DataOutputStream): 
      Option[Text] = {
    val text = super.writeText(next)(out)
    LOG.info("Text to be verified -> {}", text)
    tester ! text
    text
  } 
  
  override def toBundle[M <: Writable](messages: List[M]): 
      Option[BSPMessageBundle[M]] = {
    val combined = super.toBundle[M](messages)
    asScalaIterator(combined.iterator).foreach { bundle => 
      LOG.info("Bundle to be verified -> {}", bundle)
      asScalaIterator(bundle.iterator).foreach { m =>
        tester ! m.asInstanceOf[IntWritable].get 
      }
    }
    combined
  }

  override def markFinish(destPath: String) {
    LOG.info("Zk dest path is {} ", destPath)
    tester ! destPath
  }

}

class MockSuperstep(superstepCount: Long) extends Superstep {

   override def getVariables(): Map[String, Writable] = 
     Map("superstepCount" -> new LongWritable(superstepCount))
   override def compute(peer: org.apache.hama.bsp.v2.BSPPeer) { }
   override def next: Class[_ <: Superstep] = null

}

@RunWith(classOf[JUnitRunner])
class TestCheckpointer extends TestEnv("TestCheckpointer") with LocalZooKeeper 
                                                           with JobUtil {
/*
  val rootPath = "/bsp/checkpoint"
*/
  val superstepCount: Long = 1654
  val threshold = BSPMessageCompressor.threshold(Option(testConfiguration))
/*
  val jobIdentifier = "test"
  val id = 9
  val jobId = createJobId(jobIdentifier, id).toString
*/
  val taskAttemptId = createTaskAttemptId("test", 9, 3, 2).toString
/*
  val path = "%s/%s/%s/%s".format(rootPath, jobId, superstepCount, 
                                  taskAttemptId)
  val ckptPath = path+".ckpt"
  val zkCkptPath = path+".ok"

  val peer1 = Peer.at("testActorSystem@host14:9274")
  val msg1 = new Text("Book: A Report on the Banality of Evil")
  val peer2 = Peer.at("testActorSystem@host35:8245")
  val msg2 = new Text("The wanderer")
  val peer3 = Peer.at("testActorSystem@local:33219")
  val msg3 = new Text("Programming Erlang")

  val first = PeerAndMessages(ckptPath, peer1, msg1)
  val second = PeerAndMessages(ckptPath, peer2, msg2)
  val third = PeerAndMessages(ckptPath, peer3, msg3)
*/

  override protected def beforeAll = launchZk

  override protected def afterAll = {
    closeZk
    super.afterAll
  }

/*
  def putVariables(superstep: Superstep, key: String, value: Writable): 
      Superstep = {
    superstep.collect(key, value)
    superstep
  }
*/

  def messages(): List[Writable] = List[Writable](new IntWritable(192), 
                                                  new IntWritable(112), 
                                                  new IntWritable(23))

  it("test checkpointer.") {
    val superstep = new MockSuperstep(superstepCount)
/*
    putVariables(superstep, "count", new LongWritable(superstepCount))
    putVariables(superstep, "flag", new BooleanWritable(true))
    LOG.info("Test checkpointer for task attempt id "+taskAttemptId+
             " at superstep "+superstepCount) 
*/
    // no specific setting for task, so use testConfiguration instead.
    val ckpt = createWithArgs("checkpointer", classOf[MockCheckpointer], 
                              testConfiguration, testConfiguration, 
                              taskAttemptId, superstepCount, tester)

    ckpt ! Checkpoint(superstep.getVariables, superstep.next, messages)

    expect(superstepCount)
    expect(None)
    expect(192)
    expect(112)
    expect(23)
    expect(MockCheckpointer.tmpRootPath+
           "/job_test_0009/1654/attempt_test_0009_000003_2.ok")
/*
    val outgoing = OutgoingMessageManager.get[Writable](testConfiguration)
    outgoing.addMessage(peer1, msg1)
    outgoing.addMessage(peer2, msg2)
    outgoing.addMessage(peer3, msg3)

    asScalaIterator(outgoing.getBundleIterator).foreach ( entry => {
      val peer = entry.getKey
      val bundle = entry.getValue
      ckpt ! SavePeerMessages[Writable](peer, bundle)
    })

    // verify peer/ bundle 
    expectAnyOf(first, second, third)
    expectAnyOf(first, second, third)
    expectAnyOf(first, second, third)

    val className = superstep.getClass.getName
    val variables = superstep.getVariables
    ckpt ! SaveSuperstep(className, variables)
    // verify superstep, etc.
    expectAnyOf(NameAndMap(ckptPath, className, variables.mapValues { v =>
      v.toString
    }))
    expect(zkCkptPath)
*/
    LOG.info("Done TestCheckpointer test case!")
  }
}
