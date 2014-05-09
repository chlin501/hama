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
package org.apache.hama.master

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.event.Logging
import org.apache.hama.TestEnv
import org.apache.hama.HamaConfiguration
import org.apache.hama.groom._
import org.apache.hama.bsp.BSPJobID
import org.apache.hama.bsp.v2._
import org.apache.hama.bsp.v2.IDCreator._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

class MockMaster2(conf: HamaConfiguration) extends Master(conf) {

  override def initializeServices {
    create("sched", classOf[MockScheduler])
    create("taskManager", classOf[MockTaskManager])
  }

}

class MockTaskManager(conf: HamaConfiguration, mockSched: ActorRef) 
      extends TaskManager(conf) {

  /**
   * Disable remote lookup sched.
   * Replace sched with mock sched.
   */
  override def initializeServices = {
    initializeSlots
    sched = mockSched
    request(self, TaskRequest)
  }

  /* override TaskManager's lookup mechanism. */
  override def afterLinked(proxy: ActorRef) { }
  
  override def receive = super.receive
}

class MockScheduler(conf: HamaConfiguration, tester: ActorRef) 
    extends Scheduler(conf) {

  override def dispatch(from: ActorRef, task: Task) {
    LOG.info("Task ({}) will be dispatch to {}", 
             task.getAssignedTarget, from.path.name)
    LOG.info("Target groom name: {}", task.getAssignedTarget)
    groomTaskManagers.find(p =>   
      p._1.equals(task.getAssignedTarget)
    ) match {
      case Some(found) => {
        LOG.info("Dispatching a task to "+task.getAssignedTarget)
        tester ! task.getAssignedTarget
      }
      case None => 
        throw new RuntimeException("TaskManager "+task.getAssignedTarget+
                                   " not found!")
    }
  }

  override def enrollment: Receive = {
    case GroomEnrollment(groomServerName, tm, maxTasks) => {
      if(null != tm) 
        throw new IllegalArgumentException("Parameter tm should be null!")
      val taskManager = context.actorOf(Props(classOf[MockTaskManager], 
                                              conf, 
                                              self), 
                                        groomServerName)
      groomTaskManagers ++= Map(groomServerName -> (taskManager, maxTasks))
    }
  } 
  
  override def receive = super.receive
}

@RunWith(classOf[JUnitRunner])
class TestScheduler extends TestEnv(ActorSystem("TestScheduler")) 
                    with JobUtil {

/*
  def createPassiveJob(): Job = {
    val jobId = IDCreator.newBSPJobID.withId("test_passive_sched").
                                      withId(9).build
    new Job.Builder().setId(jobId).
                      setName("test-scheduler").
                      withTaskTable.
                      build
  }
*/

  it("test schedule tasks") {
    LOG.info("Actively schedule tasks")
    val sched = createWithArgs("activeSched", 
                           classOf[MockScheduler], 
                           conf, 
                           tester)
    val job = createJob("test-active-sched", 7, "test-scheduler", 
                        Array("groom5", "groom2", "groom5", "groom9"), 4)
    sched ! GroomEnrollment("groom5", null, 3)
    sched ! GroomEnrollment("groom2", null, 2)
    sched ! GroomEnrollment("groom9", null, 7)
    sleep(1.seconds)
    sched ! Dispense(job)
    expect("groom5")
    expect("groom2")
    expect("groom5")
    expect("groom9")
  }

/*
  it("test tasks assign") {
    LOG.info("Passively schedule tasks")
    sched = createWithArgs("passiveSched", 
                           classOf[MockScheduler], 
                           testConfiguration, 
                           "groom_127.0.0.1_50000",
                           tester)
    LOG.info("MockSched and TestProb are created! sched: "+sched+
             ", ref: "+tester)
    val job = createJob("test-active-sched", 7, "test-scheduler")
    sched ! AddProxy
    expect(ProxyAdded)
    sched ! Dispense(job)
    sleep(5.seconds)
    expect("groom_127.0.0.1_50000")
  }
*/
}
