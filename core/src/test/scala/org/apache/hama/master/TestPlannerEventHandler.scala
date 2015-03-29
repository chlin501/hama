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
import org.apache.hama.bsp.v2.Job
import org.apache.hama.bsp.v2.Job.State._
import org.apache.hama.bsp.v2.Task
import org.apache.hama.bsp.v2.Task.State._
import org.apache.hama.conf.Setting
import org.apache.hama.master.Directive.Action
import org.apache.hama.master.Directive.Action._
import org.apache.hama.monitor.master.CheckpointIntegrator
import org.apache.hama.util.JobUtil
import org.apache.hama.util.Utils
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.collection.JavaConversions._

case class D1(action: Action, hostPortMatched: Boolean, active: Boolean)

trait Helper { self: Mock => 

  def build(d: Directive, h1: String, p1: Int): D1 = {
    LOG.debug("{} replies receiving Directive!", name)
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
    case d: Directive => tester ! build(d, "groom1", 1111)
  }
  override def receive = msgs orElse super.receive
}

class Groom2(tester: ActorRef) extends Mock with Helper {
  def msgs: Receive = {
    case d: Directive => tester ! build(d, "groom2", 2222)
  }
  override def receive = msgs orElse super.receive
}

class Groom3(tester: ActorRef) extends Mock with Helper {
  def msgs: Receive = {
    case d: Directive => tester ! build(d, "groom3", 3333)
  }
  override def receive = msgs orElse super.receive
}

class Groom4(tester: ActorRef) extends Mock with Helper {
  def msgs: Receive = {
    case d: Directive => tester ! build(d, "groom4", 4444)
  }
  override def receive = msgs orElse super.receive
}

class Groom5(tester: ActorRef) extends Mock with Helper {
  def msgs: Receive = {
    case d: Directive => tester ! build(d, "groom5", 5555)
  }
  override def receive = msgs orElse super.receive
}

class Groom6(tester: ActorRef) extends Mock with Helper {
  def msgs: Receive = {
    case d: Directive => tester ! build(d, "groom6", 6666)
  }
  override def receive = msgs orElse super.receive
}

class Groom7(tester: ActorRef) extends Mock with Helper {
  def msgs: Receive = {
    case d: Directive => tester ! build(d, "groom7", 7777)
  }
  override def receive = msgs orElse super.receive
}

class Groom8(tester: ActorRef) extends Mock with Helper {
  def msgs: Receive = {
    case d: Directive => tester ! build(d, "groom8", 8888)
  }
  override def receive = msgs orElse super.receive
}

class MockMaster3(tester: ActorRef) extends Mock {

  def msgs: Receive = {
    case FindGroomsToKillTasks(infos) => infos.foreach { info => 
      tester ! info.getHost + ":" + info.getPort
    }
    case FindGroomsToRestartTasks(infos) => infos.foreach { info => 
      tester ! info.getHost + ":" + info.getPort
    }
    case GetTargetRefs(infos) => 
      tester ! infos.map { info => info.getHost + ":" + info.getPort }.toSeq
    case FindTasksAliveGrooms(infos) => 
      tester ! infos.map { info => info.getHost + ":" + info.getPort }.toSeq.
                     sorted
  } 

  override def receive = msgs orElse super.receive 
}

class MockFederator1(tester: ActorRef) extends Mock {

  def msgs: Receive = {
    case askFor: AskFor => tester ! askFor
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

  val groom1 = createWithArgs("groom1", classOf[Groom1], tester)
  val groom2 = createWithArgs("groom2", classOf[Groom2], tester)
  val groom3 = createWithArgs("groom3", classOf[Groom3], tester)
  val groom4 = createWithArgs("groom4", classOf[Groom4], tester)
  val groom5 = createWithArgs("groom5", classOf[Groom5], tester)
  val groom6 = createWithArgs("groom6", classOf[Groom6], tester)
  val groom7 = createWithArgs("groom7", classOf[Groom7], tester)
  val groom8 = createWithArgs("groom8", classOf[Groom8], tester)
 
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

  val taskSize = 8

  val hostPortMatched = true
  val active = true
  val passive = false

  def mkGrooms(idxs: Int*): Array[String] = idxs.map { idx => 
    "groom%s:%d%d%d%d".format(idx, idx, idx, idx, idx)
  }.toArray

  def excludeIdxs(idxs: Int*): Seq[Int] = (for(idx <- 1 to taskSize if
    (!idxs.contains(idx))) yield idx).toSeq

  def mkD1s(idxs: Int*): Seq[D1] = idxs.map { idx => idx match {
    case i if (i <= 2) => D1(Cancel, hostPortMatched, active) 
    case i if (i > 2 && i <= taskSize) => D1(Cancel, hostPortMatched, passive) 
    case i@_ => throw new RuntimeException("Can't build D1 for index "+i+"!")
  }}.toSeq

  def grooms(): Set[ActorRef] = Set(groom1, groom2, groom3, groom4, groom5, 
    groom6, groom7, groom8) 

  def grooms(excluded: Int*): Set[ActorRef] = grooms.filterNot( groom =>
    excluded.map("groom"+_).contains(groom.path.name))

  def activeGrooms(): Array[String] = 
    mkGrooms((for(idx <- 1 to 2) yield idx):_*) 

  def passiveGrooms(): Array[String] = 
    mkGrooms((for(idx <- 3 to 8) yield idx):_*) 

  def allGrooms(excluded: Array[String]): Seq[String] = (activeGrooms ++ 
    passiveGrooms).filterNot( groom => excluded.contains(groom) ).toSeq.sorted

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

  def mapTasksToGrooms(tasks: java.util.List[Task], expectSize: Int) {
    if(expectSize != tasks.size) 
      throw new RuntimeException("Expect "+expectSize+" tasks, but "+
                                 tasks.size+" exist!")
    tasks(0).scheduleTo(host1, port1)
    tasks(1).scheduleTo(host2, port2)
    tasks(2).assignedTo(host3, port3)
    tasks(3).assignedTo(host4, port4)
    tasks(4).assignedTo(host5, port5)
    tasks(5).assignedTo(host6, port6)
    tasks(6).assignedTo(host7, port7)
    tasks(7).assignedTo(host8, port8)
  }

  def rand(prev: Int, start: Int, end: Int): Int = {
    var value: Int = prev
    do {
      value = Utils.random(start, end)
    } while(prev == value)
    value
  }

  def testCancelTasks(jobManager: JobManager, tasks: java.util.List[Task],
                      planner: PlannerEventHandler, failed: Seq[Int]) {
    for(idx <- 0 until tasks.size) failed.contains((idx+1)) match {
      case true => 
      case false => planner.cancelled(ticket(jobManager), 
        tasks(idx).getId.toString) 
    }
    verify(jobManager, { (s, t) => {
      val stoppedOrNot = t.job.allTasksStopped
      LOG.info("Are all tasks stopped? {}", stoppedOrNot)
      stoppedOrNot 
    }})
  } 

  def expectHostPort(hostPorts: String*) = for(idx <- 0 until hostPorts.size) 
    expectAnyOf(hostPorts:_*)

  def expectD1(d1s: D1*) = for(idx <- 0 until d1s.size) expectAnyOf(d1s:_*)

  it("test planner groom and task event.") {
    val setting = Setting.master
    setting.hama.setInt("bsp.tasks.max.attempts", 20)
    val jobManager = JobManager.create
    val job = createJob("test", 2, "test-planner-job", activeGrooms, taskSize)
    job.addConfiguration(setting.hama)
    val tasksAttemptId1 = job.allTasks

    mapTasksToGrooms(tasksAttemptId1, taskSize) 

    jobManager.enqueue(Ticket(client, job))
    val master = createWithArgs("mockMaster", classOf[MockMaster3], tester)
    val federator = createWithArgs("mockFederator", classOf[MockFederator1], 
                                   tester)
    val scheduler = Scheduler.create(setting.hama, jobManager)
    setting.hama.setClass("master.planner.handler", classOf[MockPlanner], 
                          classOf[PlannerEventHandler])
    val planner = PlannerEventHandler.create(setting, jobManager, master, 
                                             federator, scheduler)
    LOG.info("Test when only groom8 leaves ...")
    planner.whenGroomLeaves(host8, port8)
    expectHostPort(hostPort1, hostPort2, hostPort3, hostPort4, hostPort5, 
                   hostPort6, hostPort7)

    verify(jobManager, { (s, t) => t.job.isRecovering && 
      RESTARTING.equals(t.job.getState) })

    LOG.info("Test cancelling tasks running on groom1 ~ groom7 ...")
    planner.cancelTasks(Set(groom1, groom2, groom3, groom4, groom5, groom6, 
      groom7), Set[String]())

    expectD1(D1(Cancel, hostPortMatched, active), 
             D1(Cancel, hostPortMatched, active), 
             D1(Cancel, hostPortMatched, passive), 
             D1(Cancel, hostPortMatched, passive),
             D1(Cancel, hostPortMatched, passive), 
             D1(Cancel, hostPortMatched, passive), 
             D1(Cancel, hostPortMatched, passive))
   
    testCancelTasks(jobManager, tasksAttemptId1, planner, Seq(8))

    expect(AskFor(CheckpointIntegrator.fullName,
                  FindLatestCheckpoint(job.getId)))

    val attemptId2 = 2 
    val ckptAt1 = 1
    LOG.info("Test restarting job {} with attempt id {} ...", job.getId, 
             attemptId2)
    planner.whenRestart(job.getId, ckptAt1)
    expect(activeGrooms.toSeq) 
    verify(jobManager, { (s, t) => {
      Job.State.RUNNING.equals(t.job.getState) &&
      (ckptAt1 == t.job.getSuperstepCount) && 
      !t.job.allTasks.map { task => (attemptId2 == task.getId.getId) && 
        WAITING.equals(task.getState) }.exists(_ == false)
    }}) 
    
    LOG.info("Test when groom8 and groom7 leave ...")
    val tasksAttemptId2 = ticket(jobManager).job.allTasks
    mapTasksToGrooms(tasksAttemptId2, taskSize) 
    planner.whenGroomLeaves(host8, port8)
    expectHostPort(hostPort1, hostPort2, hostPort3, hostPort4, hostPort5, 
                   hostPort6, hostPort7)
    planner.whenGroomLeaves(host7, port7)
    val failedTasks = ticket(jobManager).job.findTasksBy(host7, port7)
    assert(1 == failedTasks.size)
    assert(attemptId2 == failedTasks(0).getId.getId) 
    assert(Task.State.FAILED.equals(failedTasks(0).getState))

    LOG.info("Test cancelling tasks running on groom1 ~ groom6 ...")
    planner.cancelTasks(Set(groom1, groom2, groom3, groom4, groom5, groom6), 
                        Set[String]())
    expectD1(D1(Cancel, hostPortMatched, active), 
             D1(Cancel, hostPortMatched, active), 
             D1(Cancel, hostPortMatched, passive), 
             D1(Cancel, hostPortMatched, passive),
             D1(Cancel, hostPortMatched, passive), 
             D1(Cancel, hostPortMatched, passive)) 
    testCancelTasks(jobManager, tasksAttemptId2, planner, Seq(8, 7))
    expect(AskFor(CheckpointIntegrator.fullName,
                  FindLatestCheckpoint(job.getId)))

    val attemptId3 = 3
    val ckptAt2 = 2 
    LOG.info("Test restarting job {} with attempt id {} ...", job.getId, 
             attemptId3)
    planner.whenRestart(job.getId, ckptAt2)
    expect(activeGrooms.toSeq) 
    verify(jobManager, { (s, t) => {
      Job.State.RUNNING.equals(t.job.getState) &&
      (ckptAt2 == t.job.getSuperstepCount) && 
      !t.job.allTasks.map { task => (attemptId3 == task.getId.getId) && 
        WAITING.equals(task.getState) }.exists(_ == false)
    }}) 

    val tasksAttemptId3 = ticket(jobManager).job.allTasks
    mapTasksToGrooms(tasksAttemptId3, taskSize) 
    val value = Utils.random(activeGrooms.length + 1, taskSize)
    val failed = tasksAttemptId3.filter( task => task.getId.getTaskID.getId == 
      value)
    LOG.info("Pick up task for simulataing task failure: {}", failed)
    assert(null != failed && !failed.isEmpty)
    LOG.info("Test when the task {} fails  ...", failed(0).getId)
    planner.whenTaskFails(failed(0).getId)
    val alive = allGrooms(mkGrooms(value))
    expect(alive)

    LOG.info("Test cancelling tasks at {} ...", alive.mkString(", "))
    planner.cancelTasks(grooms(value), Set[String]())
    expectD1(mkD1s(excludeIdxs(value):_*):_*)
    testCancelTasks(jobManager, tasksAttemptId3, planner, Seq(value)) 
    expect(AskFor(CheckpointIntegrator.fullName,
                  FindLatestCheckpoint(job.getId)))

    val attemptId4 = 4
    val ckptAt3 = 3
    LOG.info("Test restarting job {} with attempt id {} ...", job.getId, 
             attemptId4)
    planner.whenRestart(job.getId, ckptAt3)
    expect(activeGrooms.toSeq) 
    verify(jobManager, { (s, t) => {
      Job.State.RUNNING.equals(t.job.getState) &&
      (ckptAt3 == t.job.getSuperstepCount) && 
      !t.job.allTasks.map { task => (attemptId4 == task.getId.getId) && 
        WAITING.equals(task.getState) }.exists(_ == false)
    }}) 

    val tasksAttemptId4 = ticket(jobManager).job.allTasks
    mapTasksToGrooms(tasksAttemptId4, taskSize) 
    val value1 = Utils.random(activeGrooms.length + 1, taskSize)
    val value2 = rand(value1, activeGrooms.length + 1, taskSize) 
    val failedlist = tasksAttemptId4.filter( task => 
      task.getId.getTaskID.getId == value1 || 
      task.getId.getTaskID.getId == value2)
    LOG.info("Pick up task for simulataing task failure: {}",  
             failedlist.map{ f => f.getId } .mkString(", "))
    assert(null != failedlist && !failedlist.isEmpty)
    LOG.info("Test when tasks {} fails  ...", failedlist.map { f => f.getId }.
             mkString(", "))
    val firstValue = failedlist.zipWithIndex.map { case (f, idx) => { 
      planner.whenTaskFails(f.getId)
      idx match { 
        case 0 => f.getId.getTaskID.getId match {
          case `value1` => value1
          case `value2` => value2 
        }
        case _ => -1
      }
    }}.filterNot(-1==_)(0)
    // only grooms when the frist task fails will be directed to cancel tasks.
    // the second task that fails will only be marked as failed because the 
    // groom hosting the second task will detect and report task failure event.
    // if the directive received by the groom after the task failure, simply
    // log should be ok.
    val aliveGrooms = allGrooms(mkGrooms(firstValue)) 
    expect(aliveGrooms)

    LOG.info("Test cancelling tasks at {} ...", aliveGrooms.mkString(", "))
    planner.cancelTasks(grooms(firstValue), Set[String]())
    expectD1(mkD1s(excludeIdxs(value1, value2):_*):_*)
    testCancelTasks(jobManager, tasksAttemptId4, planner, Seq(value1, value2)) 
    expect(AskFor(CheckpointIntegrator.fullName,
                  FindLatestCheckpoint(job.getId)))
 

    // TODO: test single active task failure 
    //       test two active tasks failure
    //       test two active grooms failure (can't restart)

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
