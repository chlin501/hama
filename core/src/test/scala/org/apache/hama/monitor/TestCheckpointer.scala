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
import akka.actor.ActorSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.BooleanWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.MapWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.Writable
import org.apache.hama.bsp.v2.Superstep
import org.apache.hama.HamaConfiguration
import org.apache.hama.message.BSPMessageBundle
import org.apache.hama.message.OutgoingMessageManager
import org.apache.hama.message.Peer
import org.apache.hama.ProxyInfo
import org.apache.hama.TestEnv
import org.apache.hama.util.JobUtil
import org.apache.hama.zk.LocalZooKeeper
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.collection.JavaConversions._

final case class PeerAndMessages[M <: Writable](ckptPath: String, 
                                                peer: ProxyInfo,
                                                msg: M)
final case class NameAndMap(ckptPath: String, className: String, 
                            variables: Map[String, String])

class MockCheckpointer(taskConf: HamaConfiguration,
                       taskAttemptId: String,
                       superstep: Long,
                       tester: ActorRef) 
      extends Checkpointer(taskConf, taskAttemptId, superstep) {

  override def writePeerAndMessages[M <: Writable](
    ckptPath: Path, peer: ProxyInfo, bundle: BSPMessageBundle[M]
  ) { 
    LOG.info("Checkpointer path: {}, peer: {}, bundle: {}", 
             ckptPath, peer, bundle)
    var cnt = 1 
    asScalaIterator(bundle.iterator).foreach ( msg => {
      if(1 < cnt) 
        throw new RuntimeException("Message count "+cnt+" is larger than 1!")
      tester ! PeerAndMessages(ckptPath.toString, peer, msg)
      cnt += 1
    })
  }

  override def writeClassNameAndVariables(ckptPath: Path,
                                          className: Text,
                                          variables: MapWritable) {
    LOG.info("Checkpoint path to {} with className {} and variables {}", 
             ckptPath, className.toString, variables)
    val result = asScalaSet(variables.entrySet).map ( entry => 
      (entry.getKey.asInstanceOf[Text].toString, entry.getValue.toString)
    ).toMap
    LOG.info("MapWritable after converted becomes {}", result)
    tester !  NameAndMap(ckptPath.toString, className.toString, result)
  }

  override def markFinish(destPath: String) {
    LOG.info("Zk dest path is {} ", destPath)
    tester ! destPath
  }

}

class MockSuperstep extends Superstep {

   override def getVariables(): Map[String, Writable] = super.getVariables
   override def compute(peer: org.apache.hama.bsp.v2.BSPPeer) { }
   override def next: Class[_ <: Superstep] = null

}

@RunWith(classOf[JUnitRunner])
class TestCheckpointer extends TestEnv(ActorSystem("TestCheckpointer")) 
                       with LocalZooKeeper 
                       with JobUtil {
  val rootPath = "/bsp/checkpoint"
  val superstepCount: Long = 6
  val jobIdentifier = "test"
  val id = 9
  val jobId = createJobId(jobIdentifier, id).toString
  val taskAttemptId = createTaskAttemptId("test", 9, 3, 2).toString
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

  override protected def beforeAll = launchZk

  override protected def afterAll = {
    closeZk
    super.afterAll
  }

  def putVariables(superstep: Superstep, key: String, value: Writable): 
      Superstep = {
    superstep.collect(key, value)
    superstep
  }


  it("test checkpointer.") {
    val superstep = new MockSuperstep
    putVariables(superstep, "count", new LongWritable(superstepCount))
    putVariables(superstep, "flag", new BooleanWritable(true))
    LOG.info("Test checkpointer for task attempt id "+taskAttemptId+
             " at superstep "+superstepCount) 
    // no specific setting for task, so use testConfiguration instead.
    val ckpt = createWithArgs("checkpointer", classOf[MockCheckpointer], 
                              testConfiguration, taskAttemptId, 
                              superstepCount, tester)
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
    LOG.info("Done TestCheckpointer test case!")
  }
}
