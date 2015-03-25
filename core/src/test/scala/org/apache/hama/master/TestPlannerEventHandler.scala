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
import org.apache.hama.bsp.v2.Job.State._
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

trait Helper { 
  def toD1(d: Directive, h1: String, p1: Int): D1 = {
    val action = d.action 
    val host = d.task.getAssignedHost
    val port = d.task.getAssignedPort
    val hostPortMatched = (h1.equals(host) && port == p1)
    val activeOrPassive = d.task.isActive
    D1(action, hostPortMatched, activeOrPassive)
  }
}

class Groom1(tester: ActorRef) extends Mock with Helper {
  def msgs: Receive = {
    case d: Directive => tester ! toD1(d, "groom1", 1111)
  }
  override def receive = msgs orElse super.receive
}

class Groom2(tester: ActorRef) extends Mock with Helper {
  def msgs: Receive = {
    case d: Directive => tester ! toD1(d, "groom2", 2222)
  }
  override def receive = msgs orElse super.receive
}

class Groom3(tester: ActorRef) extends Mock with Helper {
  def msgs: Receive = {
    case d: Directive => tester ! toD1(d, "groom3", 3333)
  }
  override def receive = msgs orElse super.receive
}

class Groom4(tester: ActorRef) extends Mock with Helper {
  def msgs: Receive = {
    case d: Directive => tester ! toD1(d, "groom4", 4444)
  }
  override def receive = msgs orElse super.receive
}

class Groom5(tester: ActorRef) extends Mock with Helper {
  def msgs: Receive = {
    case d: Directive => tester ! toD1(d, "groom5", 5555)
  }
  override def receive = msgs orElse super.receive
}

class Groom6(tester: ActorRef) extends Mock with Helper {
  def msgs: Receive = {
    case d: Directive => tester ! toD1(d, "groom6", 6666)
  }
  override def receive = msgs orElse super.receive
}

class Groom7(tester: ActorRef) extends Mock with Helper {
  def msgs: Receive = {
    case d: Directive => tester ! toD1(d, "groom7", 7777)
  }
  override def receive = msgs orElse super.receive
}

class Groom8(tester: ActorRef) extends Mock with Helper {
  def msgs: Receive = {
    case d: Directive => tester ! toD1(d, "groom8", 8888)
  }
  override def receive = msgs orElse super.receive
}

class MockMaster3(tester: ActorRef) extends Mock {

  def msgs: Receive = {
    case FindGroomsToKillTasks(infos) => infos.foreach { info => 
      tester !  info.getHost + ":" + info.getPort
    }
    case FindGroomsToRestartTasks(infos) => infos.foreach { info => 
      tester !  info.getHost + ":" + info.getPort
    }
/*
    case FindTasksAliveGrooms(infos) => infos.foreach { info => 
      tester !  info.getHost + ":" + info.getPort
    }
*/
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
    case "groom5" => ("groom5", 5555)
    case "groom6" => ("groom6", 6666)
    case "groom7" => ("groom7", 7777)
    case "groom8" => ("groom8", 8888)
    case _ => (null, -1)
  }

}

@RunWith(classOf[JUnitRunner])
class TestPlannerEventHandler extends TestEnv("TestPlannerEventHandler") 
                              with JobUtil {

  val host1 = "groom1"
  val port1 = 1111
  val hostPort1 = bind(host1, port1)

  val host2 = "groom2"
  val port2 = 2222
  val hostPort2 = bind(host2, port2)

  val host3 = "groom3"
  val port3 = 3333 
  val hostPort3 = bind(host3, port3)

  val host4 = "groom4"
  val port4 = 4444
  val hostPort4 = bind(host4, port4)

  val host5 = "groom5"
  val port5 = 5555
  val hostPort5 = bind(host5, port5)

  val host6 = "groom6"
  val port6 = 6666
  val hostPort6 = bind(host6, port6)

  val host7 = "groom7"
  val port7 = 7777
  val hostPort7 = bind(host7, port7)

  val host8 = "groom8"
  val port8 = 8888
  val hostPort8 = bind(host8, port8)

  val m = true

  val active = true
  val passive = false

  def activeGrooms(): Array[String] = Array(host1+":"+port1, host2+":"+port2)

  def bind(host: String, port: Int): String = host + ":" + port

  def verify(manager: JobManager, f:(Stage, Ticket) => Boolean) = 
    manager.ticketAt match {
      case (s: Some[Stage], t: Some[Ticket]) => assert(f(s.get, t.get))
      case _ => throw new RuntimeException("Invalid ticket or stage!")
    }

  def ticket(jobManager: JobManager): Ticket = jobManager.ticketAt match {
    case (s: Some[Stage], t: Some[Ticket]) => t.get
    case _ => throw new RuntimeException("Invalid ticket or stage!")
  }

  def notAllTasksStopped(mgr: JobManager, n: Int) = verify(mgr, { (s, t) => {
    val notAllStopped = !t.job.allTasksStopped
    LOG.info("Check task #{}, not all task stopped? {}", n, notAllStopped)
    notAllStopped 
  }})

  def allTasksStopped(mgr: JobManager, n: Int) = verify(mgr, { (s, t) => {
    val allStopped = t.job.allTasksStopped
    LOG.info("Check task #{}, all task stopped? {}", n, allStopped)
    allStopped 
  }})

  it("test planner groom and task event.") {
    val setting = Setting.master
    val jobManager = JobManager.create
    val job = createJob("test", 2, "test-planner-job", activeGrooms, 8) 
    val tasks = job.allTasks

    tasks(0).scheduleTo(host1, port1)
    tasks(1).scheduleTo(host2, port2)
    tasks(2).assignedTo(host3, port3)
    tasks(3).assignedTo(host4, port4)
    tasks(4).assignedTo(host5, port5)
    tasks(5).assignedTo(host6, port6)
    tasks(6).assignedTo(host7, port7)
    tasks(7).assignedTo(host8, port8)

    jobManager.enqueue(Ticket(client, job))
    val master = createWithArgs("mockMaster", classOf[MockMaster3], tester)
    val federator = createWithArgs("mockFederator", classOf[MockFederator1], 
                                   tester)
    val scheduler = Scheduler.create(setting.hama, jobManager)
    setting.hama.setClass("master.planner.handler", classOf[MockPlanner], 
                          classOf[PlannerEventHandler])
    val planner = PlannerEventHandler.create(setting, jobManager, master, 
                                             federator, scheduler)
    planner.whenGroomLeaves(host8, port8)
    expectAnyOf(hostPort1, hostPort2, hostPort3, hostPort4, hostPort5, 
                hostPort6, hostPort7)
    expectAnyOf(hostPort1, hostPort2, hostPort3, hostPort4, hostPort5, 
                hostPort6, hostPort7)
    expectAnyOf(hostPort1, hostPort2, hostPort3, hostPort4, hostPort5, 
                hostPort6, hostPort7)
    expectAnyOf(hostPort1, hostPort2, hostPort3, hostPort4, hostPort5, 
                hostPort6, hostPort7)
    expectAnyOf(hostPort1, hostPort2, hostPort3, hostPort4, hostPort5, 
                hostPort6, hostPort7)
    expectAnyOf(hostPort1, hostPort2, hostPort3, hostPort4, hostPort5, 
                hostPort6, hostPort7)
    expectAnyOf(hostPort1, hostPort2, hostPort3, hostPort4, hostPort5, 
                hostPort6, hostPort7)

    verify(jobManager, { (s, t) => t.job.isRecovering && 
      RESTARTING.equals(t.job.getState) })

    val groom1 = createWithArgs("groom1", classOf[Groom1], tester)
    val groom2 = createWithArgs("groom2", classOf[Groom2], tester)
    val groom3 = createWithArgs("groom3", classOf[Groom3], tester)
    val groom4 = createWithArgs("groom4", classOf[Groom4], tester)
    val groom5 = createWithArgs("groom5", classOf[Groom5], tester)
    val groom6 = createWithArgs("groom6", classOf[Groom6], tester)
    val groom7 = createWithArgs("groom7", classOf[Groom7], tester)
    val matched = Set(groom1, groom2, groom3, groom4, groom5, groom6, groom7) 

    planner.cancelTasks(matched, Set[String]())

    expectAnyOf(D1(Cancel, m, active), D1(Cancel, m, active), 
                D1(Cancel, m, passive), D1(Cancel, m, passive),
                D1(Cancel, m, passive), D1(Cancel, m, passive),
                D1(Cancel, m, passive)) 
    expectAnyOf(D1(Cancel, m, active), D1(Cancel, m, active), 
                D1(Cancel, m, passive), D1(Cancel, m, passive),
                D1(Cancel, m, passive), D1(Cancel, m, passive),
                D1(Cancel, m, passive)) 
    expectAnyOf(D1(Cancel, m, active), D1(Cancel, m, active), 
                D1(Cancel, m, passive), D1(Cancel, m, passive),
                D1(Cancel, m, passive), D1(Cancel, m, passive),
                D1(Cancel, m, passive)) 
    expectAnyOf(D1(Cancel, m, active), D1(Cancel, m, active), 
                D1(Cancel, m, passive), D1(Cancel, m, passive),
                D1(Cancel, m, passive), D1(Cancel, m, passive),
                D1(Cancel, m, passive)) 
    expectAnyOf(D1(Cancel, m, active), D1(Cancel, m, active), 
                D1(Cancel, m, passive), D1(Cancel, m, passive),
                D1(Cancel, m, passive), D1(Cancel, m, passive),
                D1(Cancel, m, passive)) 
    expectAnyOf(D1(Cancel, m, active), D1(Cancel, m, active), 
                D1(Cancel, m, passive), D1(Cancel, m, passive),
                D1(Cancel, m, passive), D1(Cancel, m, passive),
                D1(Cancel, m, passive)) 
    expectAnyOf(D1(Cancel, m, active), D1(Cancel, m, active), 
                D1(Cancel, m, passive), D1(Cancel, m, passive),
                D1(Cancel, m, passive), D1(Cancel, m, passive),
                D1(Cancel, m, passive)) 
   
    planner.cancelled(ticket(jobManager), tasks(0).getId.toString)  
    notAllTasksStopped(jobManager, 1)
    planner.cancelled(ticket(jobManager), tasks(1).getId.toString)  
    notAllTasksStopped(jobManager, 2)
    planner.cancelled(ticket(jobManager), tasks(2).getId.toString)  
    notAllTasksStopped(jobManager, 3)
    planner.cancelled(ticket(jobManager), tasks(3).getId.toString)  
    notAllTasksStopped(jobManager, 4)
    planner.cancelled(ticket(jobManager), tasks(4).getId.toString)  
    notAllTasksStopped(jobManager, 5)
    planner.cancelled(ticket(jobManager), tasks(5).getId.toString)  
    notAllTasksStopped(jobManager, 6)
    planner.cancelled(ticket(jobManager), tasks(6).getId.toString)  
    allTasksStopped(jobManager, 7)

/*

  
    val newTask3 = tasks(3).newWithCancelledState
    planner.renew(newTask3)

    val newTasks = job.findTasksBy(host4, port4)
    assert(null != newTasks && 1 == newTasks.size)
    LOG.info("Updated task {}", newTasks(0))
    assert(CANCELLED.equals(newTasks(0).getState))

    val oldTasks = job.findTasksNotIn(host4, port4)
    assert(null != oldTasks && 3 == oldTasks.size)
    oldTasks.foreach( oldTask => oldTask.getId.getTaskID.getId match {
      case 1 => assert(FAILED.equals(oldTask.getState)) 
      case _ => assert(WAITING.equals(oldTask.getState))
    })

    planner.whenTaskFails(tasks(1).getId)
    jobManager.ticketAt match {
      case (s: Some[Stage], t: Some[Ticket]) => 
        LOG.info("Job state is {}", t.get.job.getState)
      case _ => throw new RuntimeException("Invalid ticket or stage!")
    }
*/
    
    LOG.info("Done testing Planner event handler!")    
  }
  
}
