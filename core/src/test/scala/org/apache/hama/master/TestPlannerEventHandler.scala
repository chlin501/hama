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
import org.apache.hama.Mock
import org.apache.hama.TestEnv
import org.apache.hama.conf.Setting
import org.apache.hama.util.JobUtil
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

case class Msg(msg: String)

class MockMaster3(tester: ActorRef) extends Mock {

  def msgs: Receive = {
    case Msg(msg) => 
  } 

  override def receive = msgs orElse super.receive 
}

class MockFederator1(tester: ActorRef) extends Mock {

  def msgs: Receive = {
    case Msg(msg) => 
  } 

  override def receive = msgs orElse super.receive 
}

class MockPlanner(setting: Setting, jobManager: JobManager, master: ActorRef,
                  federator: ActorRef, scheduler: Scheduler)
  extends DefaultPlannerEventHandler(setting, jobManager, master, federator, 
                                     scheduler) {

}

@RunWith(classOf[JUnitRunner])
class TestPlannerEventHandler extends TestEnv("TestPlannerEventHandler") 
                              with JobUtil {

  val host1 = "groom1"
  val port1 = 12931

  val host2 = "groom2"
  val port2 = 29141

  def activeGrooms(): Array[String] = Array("groom213:1923", "groom412:2913")

  it("test planner groom and task event.") {
    val setting = Setting.master
    val jobManager = JobManager.create
    val job = createJob("test", 2, "test-planner-job", activeGrooms, 4) 
    jobManager.enqueue(Ticket(client, job))
    val master = createWithArgs("mockMaster", classOf[MockMaster3], tester)
    val federator = createWithArgs("mockFederator", classOf[MockFederator1], 
                                   tester)
    val scheduler = Scheduler.create(setting.hama, jobManager)
    val planner = PlannerEventHandler.create(setting, jobManager, master, 
                                             federator, scheduler)
    planner.whenGroomLeave(host1, port2)
    LOG.info("Done testing Planner event handler!")    
  }

  
}
