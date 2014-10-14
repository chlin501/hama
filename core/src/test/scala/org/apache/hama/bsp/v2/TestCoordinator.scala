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
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Writable
import org.apache.hama.HamaConfiguration
import org.apache.hama.TestEnv
import org.apache.hama.bsp.TaskAttemptID
import org.apache.hama.groom.Container
import org.apache.hama.message.MessageExecutive
import org.apache.hama.sync.BarrierClient
import org.apache.hama.logging.TaskLogger
import org.apache.hama.util.JobUtil
import org.apache.hama.util.ZkUtil._
import org.apache.hama.zk.LocalZooKeeper
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration.DurationInt

class MockCoordinator(conf: HamaConfiguration, task: Task, container: ActorRef,
                      messenger: ActorRef, syncClient: ActorRef, 
                      tasklog: ActorRef, tester: ActorRef) 
    extends Coordinator(conf, task, container, messenger, syncClient, tasklog) {

  override def doExecute() {
    super.doExecute
    currentSuperstep.map { (current) => 
      LOG.info("Current superstep {} for task {}", current.path.name, 
               task.getId)
      tester ! current.path.name
    }
  }

  def log() = LOG.info("Current task {} phase {}", task.getId, task.getPhase)
 
  // task phase
  override def setupPhase() = {
    super.setupPhase 
    log
    tester ! task.getPhase
  }

  override def whenCompute(peer: BSPPeer, superstep: ActorRef) {
    super.whenCompute(peer, superstep)
    log
    tester ! task.getPhase
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

@RunWith(classOf[JUnitRunner])
class TestCoordinator extends TestEnv("TestCoordinator") with JobUtil 
                                                         with LocalZooKeeper {

  val conf1 = config(port = 61000)
  val conf2 = config(port = 61100)

  override def beforeAll {
    super.beforeAll
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

    val container = defaultContainer()
    val tasklog = tasklogOf(taskAttemptId1)

    val messenger1 = messengerOf(1, taskAttemptId1, container, tasklog) 
    val sync1 = syncClientOf("sync1", conf1, taskAttemptId1, tasklog)

    val messenger2 = messengerOf(2, taskAttemptId2, container, tasklog) 
    val sync2 = syncClientOf("sync2", conf2, taskAttemptId2, tasklog)

    val coordinator1 = coordinatorOf("coordinator1", classOf[MockCoordinator], 
                                     task1, container, messenger1, sync1, 
                                     tasklog, tester)

    val coordinator2 = coordinatorOf("coordinator2", classOf[MockCoordinator], 
                                     task2, container, messenger2, sync2, 
                                     tasklog, tester)
   
    coordinator1 ! Execute
    coordinator2 ! Execute
    expect(Task.Phase.SETUP)
    expect(Task.Phase.SETUP)
    expect(Task.Phase.COMPUTE)
    expect(Task.Phase.COMPUTE)
    expect("superstep-"+classOf[A].getName)
    expect("superstep-"+classOf[A].getName)
    

    LOG.info("Done testing Coordinator! ")
  }
}
