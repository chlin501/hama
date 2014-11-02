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
/*
import akka.actor.ActorRef
import akka.actor.Props
import akka.event.Logging
import org.apache.hama.bsp.BSPJobID
import org.apache.hama.bsp.v2._
import org.apache.hama.bsp.v2.IDCreator._
import org.apache.hama.groom._
import org.apache.hama.HamaConfiguration
import org.apache.hama.master.Directive.Action
import org.apache.hama.TestEnv
import org.apache.hama.util.JobUtil
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

class MockTaskMgr(conf: HamaConfiguration, reporter: ActorRef, 
                  mockSched: ActorRef, name: String, constraint: Int) 
      extends TaskManager(conf, reporter) {

   * Disable remote lookup sched.
   * Replace sched with mock sched.
  override def initializeServices = {
    initializeSlots(constraint)
    sched = mockSched
    request(self, TaskRequest) 
  }

  /* override TaskManager's lookup mechanism. */
  override def afterLinked(proxy: ActorRef) { }

  override def getGroomServerName: String = name
  override def getSchedulerPath: String = mockSched.path.name
  override def getMaxTasks: Int = constraint 
  
  override def receive = super.receive
}

class MockScheduler(conf: HamaConfiguration, tester: ActorRef, 
                    receptionist: ActorRef) 
    extends Scheduler(conf, receptionist) {

  override def dispatch(from: ActorRef, action: Action, task: Task) {
    LOG.debug("Task will be dispatched to {} via actor {}", 
             task.getAssignedTarget, from.path.name)
    groomTaskManagers.find(p =>   
      p._1.equals(task.getAssignedTarget)
    ) match {
      case Some(found) => {
        LOG.info("Target groom server {} is found. Sending message {} ...", 
                 found, task.getAssignedTarget)
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
      val reporter = 
         context.actorOf(Props(classOf[org.apache.hama.groom.Reporter],
                               configuration))
      val taskManager = context.actorOf(Props(classOf[MockTaskMgr], 
                                              conf, 
                                              reporter,
                                              self,
                                              groomServerName,
                                              maxTasks), 
                                        groomServerName)
      groomTaskManagers ++= Map(groomServerName -> (taskManager, maxTasks))
    }
  } 
  
  override def receive = super.receive
}

@RunWith(classOf[JUnitRunner])
class TestScheduler extends TestEnv("TestScheduler") with JobUtil {

  it("test schedule tasks") {
    LOG.info("Test scheduler active and passive functions ...")
    val receptionist = createWithArgs("receptionist", classOf[Receptionist], 
                                      testConfiguration)
    val sched = createWithArgs("TestScheduler", 
                           classOf[MockScheduler], 
                           testConfiguration, 
                           tester, 
                           receptionist)
    val job = createJob("test-active-sched", 7, "test-scheduler", 
                        Array("groom5", "groom2", "groom5", "groom9"), 7)
    sched ! GroomEnrollment("groom5", null, 2)
    sched ! GroomEnrollment("groom2", null, 3)
    sched ! GroomEnrollment("groom9", null, 7)
    sleep(1.seconds)
    sched ! Dispense(job)
    expect("groom5")
    expect("groom2")
    expect("groom5")
    expect("groom9")
    var count = 0
    sleep(8.seconds)
    LOG.info("The rest of the message should be either groom2 or groom9, "+
             "but not groom5!")
    expectAnyOf("groom2", "groom9")
    expectAnyOf("groom2", "groom9")
    expectAnyOf("groom2", "groom9")
  }
}
*/
