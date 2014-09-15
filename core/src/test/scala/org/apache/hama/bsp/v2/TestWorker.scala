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
import org.apache.hama.bsp.v2.Task._
import org.apache.hama.groom.BSPPeerContainer
import org.apache.hama.HamaConfiguration
import org.apache.hama.logging.TaskLog
import org.apache.hama.logging.TaskLogger
import org.apache.hama.message.PeerMessenger
import org.apache.hama.TestEnv
import org.apache.hama.util.JobUtil
import org.apache.hama.zk.LocalZooKeeper
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

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

class MockWorker1(conf: HamaConfiguration, container: ActorRef, 
                  peerMessenger: ActorRef, tasklog: ActorRef, tester: ActorRef) 
      extends Worker(conf, container, peerMessenger, tasklog) {

  var captured = Map.empty[String, Superstep] 

  override def cleanup(peer: BSPPeer) {
    super.cleanup(peer)
    TaskOperator.execute(taskOperator, { (task) =>  {
      val phase = task.getPhase
      LOG.info("Phase {} should be CLEANUP right now.", phase)
      tester ! phase
      val state = task.getState
      LOG.info("State {} should be SUCCEEDED right now.", state)
      tester ! state
      val finishTime = task.getFinishTime
      LOG.info("This task is finished at {}", finishTime)
      if(0 >= finishTime) 
        throw new RuntimeException("Illegal task finish time! "+finishTime)
    }})
  }
  
  override def setup(peer: BSPPeer) {
    super.setup(peer)
    TaskOperator.execute(taskOperator, { (task) => {
      val startTime = task.getStartTime
      LOG.info("This task is started at {}", startTime)
      if(0 >= startTime) 
        throw new RuntimeException("Illegal task start time! "+startTime)
      val phase = task.getPhase 
      tester ! phase 
      LOG.info("Phase {} should SETUP right now.", phase)
      val state = task.getState 
      LOG.info("State {} should RUNNING right now.", state)
      tester ! state  
    }})
  }

  override def doExecute(taskAttemptId: String, conf: HamaConfiguration, 
                         taskConf: HamaConfiguration) = {
    setup(peer)
    bsp(peer)
    captured = asInstanceOf[SuperstepBSP].supersteps 
    LOG.info("Captured supersteps is "+captured)
  }

  override def beforeCompute(peer: BSPPeer, superstep: Superstep) {
    super.beforeCompute(peer, superstep)
    TaskOperator.execute(taskOperator, { (task) => {
      val phase = task.getPhase
      LOG.info("Phase {} should COMPUTE right now.", phase)
      tester ! phase
    }})
  }

  override def whenSync(peer: BSPPeer, superstep: Superstep) {
    super.whenSync(peer, superstep)
    TaskOperator.execute(taskOperator, { (task) => {
      val phase = task.getPhase
      LOG.info("Phase {} should be BARRIER_SYNC right now.", phase)
      tester ! phase
    }}) 
  }
  
  def getCount: Receive = {
    case GetCount =>  {
      captured.get(classOf[C].getName) match {
        case Some(found) => {
          val count = found.find[IntWritable]("count")
          LOG.info("What is the count value in variables map? {}", count)
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

  def slotSeq: Int = testConfiguration.getInt("bsp.child.slot.seq", 98)

  it("test bsp worker and task status function.") {
     val id = identifier(testConfiguration)
     val tasklog = createWithArgs("taskLogger"+slotSeq, classOf[TaskLogger],
                                  testRootPath+"/tasklogs")
     val peerMessenger = createWithArgs("peerMessenger_"+id, 
                                        classOf[PeerMessenger],
                                        testConfiguration)
     val container = createWithArgs("container", classOf[BSPPeerContainer],
                                    testConfiguration)
     // change checkpointer's root path to /tmp/hama/ckpt dir
     testConfiguration.set("bsp.checkpoint.root.path", "/tmp/hama/ckpt")
     val task = createTask("workerTask", 1, 1, 1, testConfiguration)
     val worker = createWithArgs("testWorker", classOf[MockWorker1], 
                                 testConfiguration, container, peerMessenger, 
                                 tasklog, tester)
     worker ! ConfigureFor(task)
     worker ! Execute(task.getId.toString, 
                      testConfiguration, 
                      task.getConfiguration)
     expect(Phase.SETUP)
     expect(State.RUNNING)

     // A
     expect(Phase.COMPUTE) 
     expect(Phase.BARRIER_SYNC)
     // B
     expect(Phase.COMPUTE)
     expect(Phase.BARRIER_SYNC)
     // C
     expect(Phase.COMPUTE)
     expect(Phase.BARRIER_SYNC)
     // A
     expect(Phase.COMPUTE) 
     expect(Phase.BARRIER_SYNC)
     // final step 
     expect(Phase.COMPUTE) // when next is null, no sync will be ran 

     expect(Phase.CLEANUP)
     expect(State.SUCCEEDED)
     
     worker ! GetCount
     expect(2)
     LOG.info("Wait 10 seconds for checkpoint finished! ")
     sleep(10.seconds)
     LOG.info("Done testing BSP Worker!")
  }
}
