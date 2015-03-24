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
import org.apache.hama.bsp.v2.Task
import org.apache.hama.bsp.v2.Task.State._
import org.apache.hama.conf.Setting
import org.apache.hama.master.Directive.Action
import org.apache.hama.master.Directive.Action._
import org.apache.hama.util.JobUtil
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.collection.JavaConversions._

case class Msg(msg: String)
case class D1(action: Action, hostPortMatched: Boolean, active: Boolean)

class Groom1(tester: ActorRef) extends Mock {
  def msgs: Receive = {
    case d: Directive => LOG.info("{} receives directive {}", name, d)
  }
  override def receive = msgs orElse super.receive
}

class Groom2(tester: ActorRef) extends Mock {
  def msgs: Receive = {
    case d: Directive => {
      LOG.info("Groom2 {} receives directive {}", name, d)
      val cancel = d.action 
      val host = d.task.getAssignedHost
      val port = d.task.getAssignedPort
      val matched = ("groom2".equals(host) && port == 2222)
      val active = d.task.isActive
      tester ! D1(cancel, matched, active)
    }
  }
  override def receive = msgs orElse super.receive
}

class Groom3(tester: ActorRef) extends Mock {
  def msgs: Receive = {
    case d: Directive => {
      LOG.info("Groom3 {} receives directive {}", name, d)
      val cancel = d.action 
      val host = d.task.getAssignedHost
      val port = d.task.getAssignedPort
      val matched = ("groom3".equals(host) && port == 3333)
      val active = d.task.isActive
      tester ! D1(cancel, matched, active)
    }
  }
  override def receive = msgs orElse super.receive
}

class Groom4(tester: ActorRef) extends Mock {
  def msgs: Receive = {
    case d: Directive => {
      LOG.info("Groom4 {} receives directive {}", name, d)
      val cancel = d.action 
      val host = d.task.getAssignedHost
      val port = d.task.getAssignedPort
      val matched = ("groom4".equals(host) && port == 4444)
      val active = d.task.isActive
      tester ! D1(cancel, matched, active)
    }
  }
  override def receive = msgs orElse super.receive
}

class MockMaster3(tester: ActorRef) extends Mock {

  def msgs: Receive = {
    case FindGroomsToKillTasks(infos) => infos.foreach { info => {
      val host = info.getHost
      val port = info.getPort
      LOG.info("Grooms that are still alive include {}:{}", host, port)
      tester ! host+":"+port
    }}
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

  override def resolve(ref: ActorRef): (String, Int) = ref.path.name match {
    case "groom1" => ("groom1", 1111)
    case "groom2" => ("groom2", 2222)
    case "groom3" => ("groom3", 3333)
    case "groom4" => ("groom4", 4444)
    case _ => (null, -1)
  }

}

@RunWith(classOf[JUnitRunner])
class TestPlannerEventHandler extends TestEnv("TestPlannerEventHandler") 
                              with JobUtil {

  val host1 = "groom1"
  val port1 = 1111

  val host2 = "groom2"
  val port2 = 2222

  val host3 = "groom3"
  val port3 = 3333 

  val host4 = "groom4"
  val port4 = 4444

  val hostPortMatched = true

  val active = true
  val passive = false

  def activeGrooms(): Array[String] = Array(host1+":"+port1, host2+":"+port2)

  it("test planner groom and task event.") {
    val setting = Setting.master
    val jobManager = JobManager.create
    val job = createJob("test", 2, "test-planner-job", activeGrooms, 4) 
    val tasks = job.allTasks
    tasks(0).scheduleTo(host1, port1)
    tasks(1).scheduleTo(host2, port2)
    tasks(2).assignedTo(host3, port3)
    tasks(3).assignedTo(host4, port4)
    jobManager.enqueue(Ticket(client, job))
    val master = createWithArgs("mockMaster", classOf[MockMaster3], tester)
    val federator = createWithArgs("mockFederator", classOf[MockFederator1], 
                                   tester)
    val scheduler = Scheduler.create(setting.hama, jobManager)
    setting.hama.setClass("master.planner.handler", classOf[MockPlanner], 
                          classOf[PlannerEventHandler])
    val planner = PlannerEventHandler.create(setting, jobManager, master, 
                                             federator, scheduler)
    planner.whenGroomLeave(host1, port1)

    expectAnyOf(host2+":"+port2, host3+":"+port3, host4+":"+port4)
    expectAnyOf(host2+":"+port2, host3+":"+port3, host4+":"+port4)
    expectAnyOf(host2+":"+port2, host3+":"+port3, host4+":"+port4)

    val groom2 = createWithArgs("groom2", classOf[Groom2], tester)
    val groom3 = createWithArgs("groom3", classOf[Groom3], tester)
    val groom4 = createWithArgs("groom4", classOf[Groom4], tester)
    val matched = Set(groom2, groom3, groom4) 

    planner.cancelTasks(matched, Set[String]())

    expectAnyOf(D1(Cancel, hostPortMatched, active), 
                D1(Cancel, hostPortMatched, passive), 
                D1(Cancel, hostPortMatched, passive))
    expectAnyOf(D1(Cancel, hostPortMatched, active), 
                D1(Cancel, hostPortMatched, passive), 
                D1(Cancel, hostPortMatched, passive))
    expectAnyOf(D1(Cancel, hostPortMatched, active), 
                D1(Cancel, hostPortMatched, passive), 
                D1(Cancel, hostPortMatched, passive))
   
    val newTask3 = tasks(3).newWithCancelledState
    planner.renew(newTask3)
    val newTasks = job.findTasksBy(host4, port4)
    assert(null != newTasks && 1 == newTasks.size)
    LOG.info("Updated task {}", newTasks(0))
    assert(CANCELLED.equals(newTasks(0).getState))

    LOG.info("Done testing Planner event handler!")    
  }
  
}
