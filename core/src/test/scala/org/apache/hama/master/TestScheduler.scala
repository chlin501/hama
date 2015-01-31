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
import org.apache.hama.Periodically
import org.apache.hama.TestEnv
import org.apache.hama.Tick
import org.apache.hama.bsp.BSPJobID
import org.apache.hama.bsp.TaskAttemptID
import org.apache.hama.bsp.v2.Job
import org.apache.hama.bsp.v2.Task
import org.apache.hama.bsp.v2.Task.Phase
import org.apache.hama.bsp.v2.Task.Phase._
import org.apache.hama.bsp.v2.Task.State
import org.apache.hama.bsp.v2.Task.State._
import org.apache.hama.conf.Setting
import org.apache.hama.io.PartitionedSplit
import org.apache.hama.master.Directive.Action
import org.apache.hama.master.Directive.Action._
import org.apache.hama.groom.TaskRequest
import org.apache.hama.groom.RequestTask
import org.apache.hama.monitor.GroomStats
import org.apache.hama.monitor.SlotStats
import org.apache.hama.monitor.master.GetGroomCapacity
import org.apache.hama.monitor.master.GroomCapacity
import org.apache.hama.util.JobUtil
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

final case class Sched(sched: ActorRef)
final case class Received(action: Action, id: TaskAttemptID, start: Long,
                          finish: Long, split: PartitionedSplit, superstep: Int,
                          state: State, phase: Phase, completed: Boolean,
                          active: Boolean, assigned: Boolean, bspTasks: Int)

trait MockTC extends Mock with Periodically {// task counsellor

  var sched: Option[ActorRef] = None

  var tasks = Array.empty[Task]  

  override def preStart = tick(self, TaskRequest)

  def testMsg: Receive = {
    case Sched(sched) => this.sched = Option(sched)
    case directive: Directive => directive match {
      case null => throw new RuntimeException("Directive shouldn't be null!")
      case d@_ => received(d)
    }
  }

  override def ticked(msg: Tick): Unit = msg match {
    case TaskRequest => requestNewTask()
  }

  def received(d: Directive) { }

  def requestNewTask() = sched.map { s => s ! RequestTask(currentGroomStats) } 

  def currentGroomStats(): GroomStats = {
    val n = name
    val host = name
    val port = 50000
    val maxTasksAllowed = 3
    val slotStats = currentSlotStats
    LOG.debug("Current groom stats: name {}, host {}, port {}, maxTasks {}, "+
              "slot stats {}", n, host, port, maxTasksAllowed, slotStats)
    GroomStats(n, host, port, maxTasksAllowed, slotStats)
  } 

  def currentSlotStats(): SlotStats = 
    SlotStats(Array("none", "none", "none"), Map(), 3)


  def c(d: Directive): Received = 
    Received(d.action, d.task.getId, d.task.getStartTime, d.task.getFinishTime,
             d.task.getSplit, d.task.getCurrentSuperstep, d.task.getState,
             d.task.getPhase, d.task.isCompleted, d.task.isActive, 
             d.task.isAssigned, d.task.getTotalBSPTasks)

  def add(task: Task) = {
    tasks ++= Array(task)
    LOG.info("{} has {} tasks: {}", name, tasks.length, tasks.mkString(", "))
  }

  def doLaunch(d: Directive, f: (Received) => Unit) {
    val r = c(d)
    f(r)
    add(d.task)     
    LOG.info("Directive at {} contains {}", name, r)
  }

  override def receive = testMsg orElse tickMessage orElse super.receive
}

class Passive1(tester: ActorRef) extends MockTC {

   override def received(d: Directive) = d.action match { 
     case Launch => doLaunch(d, { r => tester ! r })
     case Cancel =>
     case Resume =>
   }
}

class Passive2(tester: ActorRef) extends MockTC {

   override def received(d: Directive) = d.action match { 
     case Launch => doLaunch(d, { r => tester ! r })
     case Cancel =>
     case Resume =>
   }
}

class Passive3(tester: ActorRef) extends MockTC {

   override def received(d: Directive) = d.action match { 
     case Launch => doLaunch(d, { r => tester ! r })
     case Cancel =>
     case Resume =>
   }
}

class Passive4(tester: ActorRef) extends MockTC {

   override def received(d: Directive) = d.action match { 
     case Launch => doLaunch(d, { r => tester ! r })
     case Cancel =>
     case Resume =>
   }
}

class Passive5(tester: ActorRef) extends MockTC {

   override def received(d: Directive) = d.action match { 
     case Launch => doLaunch(d, { r => tester ! r })
     case Cancel =>
     case Resume =>
   }
}

class Active1(tester: ActorRef) extends MockTC {

   override def received(d: Directive) = d.action match { 
     case Launch => doLaunch(d, { r => tester ! r })
     case Cancel =>
     case Resume =>
   }
}

class Active2(tester: ActorRef) extends MockTC {

   override def received(d: Directive) = d.action match { 
     case Launch => doLaunch(d, { r => tester ! r })
     case Cancel =>
     case Resume =>
   }
}

class Active3(tester: ActorRef) extends MockTC {

   override def received(d: Directive) = d.action match { 
     case Launch => doLaunch(d, { r => tester ! r })
     case Cancel =>
     case Resume =>
   }
}

class Client(tester: ActorRef) extends Mock

class Master(actives: Array[ActorRef], passives: Array[ActorRef]) extends Mock {

  def testMsg: Receive = {
    case GetTargetRefs(infos) => {
      LOG.info("{} asks for active grooms {}!", sender.path.name, 
               actives.map { a => a.path.name }.mkString(", "))
      sender ! TargetRefs(actives)
    }
    case Sched(sched: ActorRef) => {
      LOG.info("Dispatch {} to active and passive grooms!", sched.path.name)
      actives.foreach { active => active ! Sched(sched) }
      passives.foreach { passive => passive ! Sched(sched) }
    }
  }

  override def receive = testMsg orElse super.receive
}

class F extends Mock { // federator

  def testMsg: Receive = {
    case AskFor(recepiant: String, action: Any) => action match {
      case GetGroomCapacity(grooms: Array[ActorRef]) => {
        val capacity = GroomCapacity(grooms.map{ groom => (groom -> 3) }.toMap)
        LOG.info("Make-up grooms capacity {}", capacity)
        sender ! capacity
      }
      case _ => throw new RuntimeException("Unknown action "+action+"!")
    }
  }

  override def receive = testMsg orElse super.receive
}

final case class J(job: Job)
class R(client: ActorRef) extends Mock { // receptionist

  var job: Option[Job] = None

  def testMsg: Receive = {
    case J(j) => job = Option(j)
    case TakeFromWaitQueue => job match {
      case None => throw new RuntimeException("Job is not yet ready!")
      case Some(j) => {
        LOG.info("Job {} is dispensed to scheduler!", j.getId)
        sender ! Dispense(Ticket(client, j))
        job = None
      }
    }
  }

  override def receive = testMsg orElse super.receive
}

class MockScheduler(setting: Setting, master: ActorRef, receptionist: ActorRef,
                    federator: ActorRef, tester: ActorRef) 
      extends Scheduler(setting, master, receptionist, federator) {

  override def initializeServices { 
    super.initializeServices
    LOG.info("Dispatch {} reference to grooms!", self.path.name)
    master ! Sched(self)
  }

  override def targetHostPort(target: ActorRef): (String, Int) = 
    target.path.name match {
      case "active1" => ("active1", 50000)
      case "active2" => ("active2", 50000)
      case "active3" => ("active3", 50000)
      case "passive1" => ("passive1", 50000)
      case "passive2" => ("passive2", 50000)
      case "passive3" => ("passive3", 50000)
      case "passive4" => ("passive4", 50000)
      case "passive5" => ("passive5", 50000)
    }

  override def receive = super.receive

}

@RunWith(classOf[JUnitRunner])
class TestScheduler extends TestEnv("TestScheduler") with JobUtil {

  val emptySplit: PartitionedSplit = null
  val active: Boolean = true
  val passive: Boolean = false
  val assigned: Boolean = true
  val completed: Boolean = false

  val actives: Array[ActorRef] = Array(
    createWithArgs("active1", classOf[Active1], tester),
    createWithArgs("active2", classOf[Active2], tester), 
    createWithArgs("active3", classOf[Active3], tester)
  )

  val passives: Array[ActorRef] = Array(
    createWithArgs("passive1", classOf[Passive1], tester),
    createWithArgs("passive2", classOf[Passive2], tester), 
    createWithArgs("passive3", classOf[Passive3], tester),
    createWithArgs("passive4", classOf[Passive4], tester),
    createWithArgs("passive5", classOf[Passive5], tester)
  )

  def r(action: Action, id: TaskAttemptID, activeOrPassive: Boolean): Received =
    Received(action, id, 0, 0, emptySplit, 0, WAITING, SETUP, completed, 
             activeOrPassive, assigned, 8)

  def jobWithActiveGrooms(ident: String, id: Int): Job = createJob(ident, id, 
    "test-sched", Array("host123:412", "host1:1924", "host717:22123"), 8)

  def taskAttemptId(jobId: BSPJobID, taskId: Int, attemptId: Int): 
    TaskAttemptID = createTaskAttemptId(jobId, taskId, attemptId)

  it("test scheduling functions.") {
    val setting = Setting.master
    val job = jobWithActiveGrooms("test", 2)

    val taskAttemptId1 = taskAttemptId(job.getId, 1, 1)
    val taskAttemptId2 = taskAttemptId(job.getId, 2, 1)
    val taskAttemptId3 = taskAttemptId(job.getId, 3, 1)

    val taskAttemptId4 = taskAttemptId(job.getId, 4, 1)
    val taskAttemptId5 = taskAttemptId(job.getId, 5, 1)
    val taskAttemptId6 = taskAttemptId(job.getId, 6, 1)
    val taskAttemptId7 = taskAttemptId(job.getId, 7, 1)
    val taskAttemptId8 = taskAttemptId(job.getId, 8, 1)

    val client = createWithArgs("mockClient", classOf[Client], tester)
    val receptionist = createWithArgs("mockReceptionist", classOf[R], client)
    receptionist ! J(job)
    val federator = createWithArgs("mockFederator", classOf[F])
    val master = createWithArgs("mockMaster", classOf[Master], actives, 
                                passives)
    val sched = createWithArgs("mockSched", classOf[MockScheduler], setting, 
                               master, receptionist, federator, tester)

    val r1 = r(Launch, taskAttemptId1, active)
    val r2 = r(Launch, taskAttemptId2, active)
    val r3 = r(Launch, taskAttemptId3, active)

    val r4 = r(Launch, taskAttemptId4, passive)
    val r5 = r(Launch, taskAttemptId5, passive)
    val r6 = r(Launch, taskAttemptId6, passive)
    val r7 = r(Launch, taskAttemptId7, passive)
    val r8 = r(Launch, taskAttemptId8, passive)

    LOG.info("Expect directives sent to active grooms ... ")
    expectAnyOf(r1, r2, r3) 
    expectAnyOf(r1, r2, r3) 
    expectAnyOf(r1, r2, r3) 

    LOG.info("Expect directives sent to passive grooms ...")
    expectAnyOf(r4, r5, r6, r7, r8)
    expectAnyOf(r4, r5, r6, r7, r8)
    expectAnyOf(r4, r5, r6, r7, r8)
    expectAnyOf(r4, r5, r6, r7, r8)
    expectAnyOf(r4, r5, r6, r7, r8)

    // TODO: random pick up 1 passive groom to stop for simulating offline
    //       then ask master sending GroomLeave event to sched
    //       observe if active tasks are rescheduled to active grooms again, and
    //       all passive tasks are reassigned as well. 
    //       then ask one passive groom for task failure event (to sched)
    //       observe if all tasks killed and rescheduled correctly.
    //       then ask active groom task fail
    //       observe if reject to client, set job to fail, and move job to finished, etc.

    LOG.info("Done testing scheduler functions!")
  }

}
