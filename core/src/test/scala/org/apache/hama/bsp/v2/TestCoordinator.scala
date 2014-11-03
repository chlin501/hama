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
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import java.util.concurrent.LinkedBlockingQueue
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Writable
import org.apache.hama.Agent
import org.apache.hama.HamaConfiguration
import org.apache.hama.TestEnv
import org.apache.hama.bsp.TaskAttemptID
import org.apache.hama.bsp.v2.Task.Phase._
import org.apache.hama.groom.Container
import org.apache.hama.message.MessageExecutive
import org.apache.hama.sync.CuratorBarrier
import org.apache.hama.sync.CuratorRegistrator
import org.apache.hama.sync.PeerClient
import org.apache.hama.logging.TaskLogger
import org.apache.hama.logging.CommonLog
import org.apache.hama.util.JobUtil
import org.apache.hama.util.ZkUtil._
import org.apache.hama.util.Utils
import org.apache.hama.zk.LocalZooKeeper
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration.DurationInt

final case class Store(msg: Any)

class MockCoordinator(conf: HamaConfiguration, 
                      task: Task, 
                      container: ActorRef,
                      messenger: ActorRef, 
                      peer: ActorRef, 
                      tasklog: ActorRef, sequencer: ActorRef)
    extends Coordinator(conf, task, container, messenger, peer, tasklog) {

  override def doExecute() {
    super.doExecute
    currentSuperstep.map { (current) => {
      //LOG.info("[{}] Current superstep {}", task.getId, current.path.name)
      sequencer ! Store(current.path.name)
    }}
  }

  override def beforeExecuteNext(clazz: Class[_]) { 
    //LOG.info("[{}] Superstep to be executed next: {}", task.getId, 
             //clazz.getName)
    sequencer ! Store(clazz.getName)
  }
 
  // task phase
  override def setupPhase() {
    super.setupPhase 
    //LOG.info("[{}] Current phase {}", task.getId, task.getPhase)
    sequencer ! Store(task.getPhase)
  }

  override def computePhase() {
    super.computePhase
    //LOG.info("[{}] Current phase {}", task.getId, task.getPhase)
    sequencer ! Store(task.getPhase)
  }
  
  override def barrierEnterPhase() {
    super.barrierEnterPhase
    //LOG.info("[{}] Current phase {}", task.getId, task.getPhase)
    sequencer ! Store(task.getPhase)
  }

  override def withinBarrierPhase() {
    super.withinBarrierPhase
    //LOG.info("[{}] Current phase {}", task.getId, task.getPhase)
    sequencer ! Store(task.getPhase)
  }

  override def barrierLeavePhase() {
    super.barrierLeavePhase
    //LOG.info("[{}] Current phase {}", task.getId, task.getPhase)
    sequencer ! Store(task.getPhase)
  }

  override def exitBarrierPhase() {
    super.exitBarrierPhase
    //LOG.info("[{}] Current phase {}", task.getId, task.getPhase)
    sequencer ! Store(task.getPhase)
  }
  
  override def cleanupPhase() {
    super.cleanupPhase
    //LOG.info("[{}] Current phase {}", task.getId, task.getPhase)
    sequencer ! Store(task.getPhase)
  }
}

class A extends FirstSuperstep {

  var nextStep = classOf[B]

  override def compute(peer: BSPPeer) {
    find[IntWritable]("count") match {
      case null => {
        println(getClass.getName+" initializes count to 0!")
        collect[IntWritable]("count", new IntWritable(0))
      } 
      case count@_ => {
        nextStep = null
        println(getClass.getName+"'s current count: "+count+
                ", next superstep: "+nextStep)
      }
    }
  }

  override def next(): Class[_  <: Superstep] = nextStep
}

class B extends Superstep {

  override def compute(peer: BSPPeer) {
    find[IntWritable]("count") match {
      case null => throw new NullPointerException("Count shouldn't be null!")
      case count@_ => collect[IntWritable]("count", 
                                           new IntWritable(count.get + 1))
    }
  }

  override def next(): Class[_ <: Superstep] = classOf[C]

}

class C extends Superstep {

  override def compute(peer: BSPPeer) {
    find[IntWritable]("count") match {
      case null => throw new NullPointerException("Count shouldn't be null!")
      case count@_ => collect[IntWritable]("count", 
                                           new IntWritable(count.get + 1))
    }
  }

  override def next(): Class[_ <: Superstep] = classOf[A]
}

final case object Retrieve

class Sequencer(taskAttemptId: String, tester: ActorRef) extends Agent {

  var seq = Seq.empty[Any]

  def store: Receive = {
    case Store(msg) => {
      //LOG.info("Store {} for task {}", msg, taskAttemptId)
      seq ++= Seq(msg)
    }
  }

  def retrieve: Receive = {
    case Retrieve => {
      LOG.info("Retrieve stored messages {} for task {}", seq, taskAttemptId)
      sender ! seq
    }
  }

  override def receive = store orElse retrieve orElse unknown
}

/*
object Setting {

  import TestEnv._

  def toConfig(): Config = parseString("""
    testCoordinator {
      akka.actor.my-pinned-dispatcher {
        type = PinnedDispatcher
        executor = "thread-pool-executor"
      }
    }
  """)
}

@RunWith(classOf[JUnitRunner])
class TestCoordinator extends TestEnv("TestCoordinator", Setting.toConfig.
                                      getConfig("testCoordinator")) 
                      with JobUtil with LocalZooKeeper {
*/
@RunWith(classOf[JUnitRunner])
class TestCoordinator extends TestEnv("TestCoordinator") 
                      with JobUtil with LocalZooKeeper {

  //val pinned = "akka.actor.my-pinned-dispatcher"

  val numBSPTasks = 2

  val conf1 = config(port = 61000)
  val conf2 = config(port = 61100)

  override def beforeAll {
    super.beforeAll
    testConfiguration.setBoolean("bsp.checkpoint.enabled", false)
    launchZk
  }

  override protected def afterAll = {
    closeZk
    super.afterAll
  }

  def configSupersteps(conf: HamaConfiguration, 
                       classes: Class[_]*) = 
    conf.set("hama.supersteps.class", classes.map { (clazz) => clazz.getName }.
                                      mkString(","))

  it("test bsp peer coordinator function.") {
   
    val task1 = createTask() 
    configSupersteps(task1.getConfiguration, classOf[A], classOf[B], classOf[C])

    val task2 = createTask(taskId = 2) 
    configSupersteps(task2.getConfiguration, classOf[A], classOf[B], classOf[C])

    val taskAttemptId1 = task1.getId
    val taskAttemptId2 = task2.getId

    val container = createWithArgs("container", classOf[Container], testConfiguration)
    val tasklog = createWithArgs("tasklog", classOf[TaskLogger], "/tmp/hama/log", taskAttemptId1, true)

    val messengerName1 = "messenger-BSPPeerSystem1"
    val messenger1 = createWithArgs(messengerName1, classOf[MessageExecutive[Writable]], testConfiguration, 1, taskAttemptId1, container, tasklog)

    val messengerName2 = "messenger-BSPPeerSystem2"
    val messenger2 = createWithArgs(messengerName2, classOf[MessageExecutive[Writable]], testConfiguration, 2, taskAttemptId2, container, tasklog)

    val syncer1 = CuratorBarrier(conf1, taskAttemptId1, numBSPTasks)
    val reg1 = CuratorRegistrator(conf1)
    val peer1 = createWithArgs("peer1", classOf[PeerClient], conf1, taskAttemptId1, syncer1, reg1, tasklog) 

    val syncer2 = CuratorBarrier(conf2, taskAttemptId2, numBSPTasks)
    val reg2 = CuratorRegistrator(conf2)
    val peer2 = createWithArgs("peer2", classOf[PeerClient], conf2, taskAttemptId2, syncer2, reg2, tasklog) 

    val sequencer1 = createWithArgs("seq-for-task1", classOf[Sequencer], taskAttemptId1.toString, tester)
    val coordinator1 = createWithArgs("coordinator1", classOf[MockCoordinator], testConfiguration, task1, container, messenger1, peer1, tasklog, sequencer1)

    val sequencer2 = createWithArgs("seq-for-task2", classOf[Sequencer], taskAttemptId2.toString, tester)
    val coordinator2 = createWithArgs("coordinator2", classOf[MockCoordinator], testConfiguration, task2, container, messenger2, peer2, tasklog, sequencer2)
   
    coordinator1 ! Execute
    coordinator2 ! Execute

    val t = 1*60*1000
    LOG.info("Waiting for {} secs before information collected ...", (t/1000d))
    Thread.sleep(t)

    val expect_seq1 = Seq[Any](SETUP, COMPUTE, "superstep-org.apache.hama.bsp.v2.A", BARRIER_ENTER, WITHIN_BARRIER, BARRIER_LEAVE, EXIT_BARRIER, "org.apache.hama.bsp.v2.B", COMPUTE, BARRIER_ENTER, WITHIN_BARRIER, BARRIER_LEAVE, EXIT_BARRIER, "org.apache.hama.bsp.v2.C", COMPUTE, BARRIER_ENTER, WITHIN_BARRIER, BARRIER_LEAVE, EXIT_BARRIER, "org.apache.hama.bsp.v2.A", COMPUTE, CLEANUP)

    val expect_seq2 = Seq[Any](SETUP, COMPUTE, "superstep-org.apache.hama.bsp.v2.A", BARRIER_ENTER, WITHIN_BARRIER, BARRIER_LEAVE, EXIT_BARRIER, "org.apache.hama.bsp.v2.B", COMPUTE, BARRIER_ENTER, WITHIN_BARRIER, BARRIER_LEAVE, EXIT_BARRIER, "org.apache.hama.bsp.v2.C", COMPUTE, BARRIER_ENTER, WITHIN_BARRIER, BARRIER_LEAVE, EXIT_BARRIER, "org.apache.hama.bsp.v2.A", COMPUTE, CLEANUP)

    val actual_seq1 = Utils.await[Seq[Any]](sequencer1, Retrieve)
    LOG.info("Total stats collected for task {}: {}", taskAttemptId1, 
             actual_seq1)
    val actual_seq2 = Utils.await[Seq[Any]](sequencer2, Retrieve)
    LOG.info("Total stats collected for task {}: {}", taskAttemptId2, 
             actual_seq2)
    assert(expect_seq1.equals(actual_seq1))
    assert(expect_seq2.equals(actual_seq2))

    LOG.info("Done testing Coordinator! ")
  }
}

