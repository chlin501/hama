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

import akka.actor.ActorRef
import java.net.InetAddress
import org.apache.hadoop.io.IntWritable
import org.apache.hama.bsp.BSPJobID
import org.apache.hama.HamaConfiguration
import org.apache.hama.groom.BSPPeerContainer
import org.apache.hama.message.PeerMessenger
import org.apache.hama.message.LocalTarget
import org.apache.hama.TestEnv
import org.apache.hama.util.JobUtil
import org.apache.hama.zk.LocalZooKeeper
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

final case object GetCount

class A extends FirstSuperstep {

  var nextStep: Class[_ <: Superstep] = classOf[B]

  override def compute(peer: BSPPeer) {
    val cnt = find[IntWritable]("count")
    cnt match {
      case null => collect("count", new IntWritable(0))
      case int: IntWritable => nextStep = classOf[FinalSuperstep]
    } 
  } 

  override def next: Class[_ <: Superstep] = nextStep
}

class B extends Superstep {

  override def compute(peer: BSPPeer) {
    val cnt = find[IntWritable]("count")
    cnt match {
      case int: IntWritable => collect("count", new IntWritable(int.get+1))
      case what@_ => throw new RuntimeException("Expect IntWritable but found "+
                                                what)
    }
  }

  def next(): Class[_<:Superstep] = classOf[C]
}

class C extends Superstep {

  override def compute(peer: BSPPeer) {
    val cnt = find[IntWritable]("count")
    cnt match {
      case int: IntWritable => collect("count", new IntWritable(int.get+1))
      case what@_ => throw new RuntimeException("Expect IntWritable but "+what+
                                                " found!")
    }
  }

  override def next(): Class[_<:Superstep] = classOf[A]
}

class MockWorker1(container: ActorRef, peerMessenger: ActorRef, 
                  tester: ActorRef) extends Worker(container, peerMessenger) {

  var captured = Map.empty[String, Superstep] 

  override def doExecute(taskAttemptId: String, conf: HamaConfiguration, 
                         taskConf: HamaConfiguration) = {//peer match { 
    //case Some(found) => { 
      setup(peer)
      bsp(peer)
      captured = asInstanceOf[SuperstepBSP].supersteps 
      LOG.info("Captured supersteps is "+captured)
    //} 
    //case None => LOG.warning("BSPPeer is missing!")
  }
  
  def getCount: Receive = {
    case GetCount =>  {
      captured.get(classOf[C].getName) match {
        case Some(found) => {
          val count = found.find[IntWritable]("count")
          tester ! count.get
        }
        case None => throw new RuntimeException("Superstep C not found!")
      }
    }
  } 
  
  override def receive = getCount orElse super.receive
}

@RunWith(classOf[JUnitRunner])
class TestWorker extends TestEnv("TestWorker") with JobUtil 
                                               with LocalZooKeeper {

  override def beforeAll { 
    super.beforeAll    
    launchZk
    val classes = "%s,%s,%s,%s".format(classOf[A].getName,
                                    classOf[B].getName,
                                    classOf[C].getName,
                                    classOf[FinalSuperstep].getName)
    testConfiguration.set("hama.supersteps.class", classes)
    testConfiguration.setInt("bsp.child.slot.seq", 98)
    configure(testConfiguration)
  }

  override def afterAll {
    closeZk
    super.afterAll
  }

  def configure(conf: HamaConfiguration,
                seq: Int = 1,
                numPeers: Int = 1, // N.B.: use 1 or this test will be blocked!
                port: Int = 61000): HamaConfiguration = {
    conf.setInt("bsp.child.slot.seq", seq)
    conf.set("hama.zookeeper.quorum", "localhost:2181")
    conf.setInt("hama.zookeeper.property.clientPort", 2181)
    conf.setInt("bsp.peers.num", numPeers)
    conf.set("bsp.peer.hostname", InetAddress.getLocalHost.getHostName)
    conf.setInt("bsp.peer.port", port)
    conf
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

  it("test bsp worker function.") {
     val id = identifier(testConfiguration)
     val peerMessenger = createWithArgs("peerMessenger_"+id, 
                                        classOf[PeerMessenger])
     val container = createWithArgs("container", classOf[BSPPeerContainer])
     val task = createTask("testworker", 1, 1, 1, testConfiguration)
     val worker = createWithArgs("testWorker", classOf[MockWorker1], 
                                 container, peerMessenger, tester)
     peerMessenger ! LocalTarget(worker)
     //worker ! Bind(testConfiguration, system)
     worker ! ConfigureFor(testConfiguration, task)
     worker ! Execute(task.getId.toString, 
                      testConfiguration, 
                      task.getConfiguration)
     worker ! GetCount
     expect(2)
     LOG.info("Done testing BSP Worker!")
  }
}
