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
      println("Current superstep name "+current.path.name)
      tester ! current.path.name
    }
  }

}

class A extends FirstSuperstep {

  var nextStep = classOf[B]

  override def compute(peer: BSPPeer) {
    find[IntWritable]("count") match {
      case null => {
        println("AAAAAAAAAAAAAAA initialize count to 0!")
        collect[IntWritable]("count", new IntWritable(0))
      } 
      case count@_ => {
        nextStep = null
        println("BBBBBBBBBBBBB current count: "+count+" next step: "+nextStep)
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

  override def beforeAll {
    super.beforeAll
    configSupersteps(classOf[A], classOf[B], classOf[C])
    launchZk
  }

  override protected def afterAll = {
    closeZk
    super.afterAll
  }

  def configSupersteps(classes: Class[_]*) = classes.foreach( (clazz) =>
    testConfiguration.setClass("hama.supersteps.class", clazz, 
                               classOf[Superstep])
  )

  it("test bsp peer coordinator function.") {
    val task = createTask() 
    val taskAttemptId = task.getId
    val container = defaultContainer()
    val tasklog = tasklogOf(taskAttemptId)

    val messenger1 = messengerOf(1, taskAttemptId, container, tasklog) 
    val sync1 = syncClientOf("sync1", taskAttemptId, tasklog)

    //val coordinator = coordinator(task, container, msgmgr, syncer, 
                                        //tasklog)

    LOG.info("(Not yet implemetned) Done testing Coordinator! ")
  }
}
