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
import akka.actor.Terminated
import java.util.concurrent.BlockingQueue
import java.util.concurrent.Callable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.ExecutorService
import java.util.concurrent.LinkedBlockingQueue
import org.apache.hama.Close
import org.apache.hama.Mock
import org.apache.hama.Periodically
import org.apache.hama.SystemInfo
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
import scala.collection.JavaConversions._
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration
import scala.util.Failure
import scala.util.Success
import scala.util.Try

final case class Refs(sched: ActorRef, m: ActorRef)
final case class Received(action: Action, id: TaskAttemptID, start: Long,
                          finish: Long, split: PartitionedSplit, 
                          superstep: Long, state: State, phase: Phase, 
                          completed: Boolean, active: Boolean, 
                          assigned: Boolean, bspTasks: Int)
final case class OfflinePassive(n: Int)
final case class CurrentState(jobId: String, jobState: String,
                              tasksState: String)
final case class Groom(name: String, taskSize: Int)
final case object CalculateGroomTaskSize
final case class AttemptId(id: Int)
final case class JobId(jobId: String, from: String)
final case class UpdatedJob(jobId: BSPJobID, jobName: String, 
                            maxTaskAttempts: Int, state: String,
                            latestCheckpoint: Long, activeGrooms: String)
final case class UpdatedTask(jobId: TaskAttemptID, assigned: Boolean,
                             assignedHost: String, assignedPort: Int,
                             totalBSPTasks: Int)

trait MockTC extends Mock with Periodically {// task counsellor

  var scheduler: Option[ActorRef] = None

  var master: Option[ActorRef] = None

  var tester: Option[ActorRef] = None

  var tasks = Array.empty[Task]  

  var queue: Option[BlockingQueue[Groom]] = None

  def tasksLength(): Int = tasks.size

  override def preStart = tick(self, TaskRequest)

  def testMsg: Receive = {
    case Refs(sched, m) => {
      this.scheduler = Option(sched)
      this.master = Option(m)
    }
    case directive: Directive => directive match {
      case null => throw new RuntimeException("Directive shouldn't be null!")
      case d@_ => received(d)
    }
    case CalculateGroomTaskSize => queue.map { q => 
      q.put(Groom(name, tasksLength)) 
    }
  }

  override def ticked(msg: Tick): Unit = msg match {
    case TaskRequest => requestNewTask()
  }

  def received(d: Directive) { }

  def requestNewTask() = scheduler.map { sched => 
    sched ! RequestTask(currentGroomStats) 
  }

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

  def add(task: Task) {
    tasks ++= Array(task)
    LOG.debug("[add] {} has {} tasks: {}", name, tasks.length, 
              tasks.mkString(", "))
  }

  def remove(task: Task) {
    tasks = tasks diff Array(task)
    LOG.debug("[remove] {} has {} tasks: {}", name, tasks.length, 
              tasks.mkString(", "))
  }

  def doLaunch(d: Directive) {
    val r = c(d)
    tester.map { t => t ! r }
    add(d.task)     
    LOG.debug("Directive at {} now has {} tasks!", name, tasksLength)
  }

  def doCancel(d: Directive) {
    tester.map { t => t ! AttemptId(d.task.getId.getId) }
    scheduler.map { sched => sched ! TaskCancelled(d.task.getId.toString) }
    remove(d.task)
  }

  override def receive = testMsg orElse tickMessage orElse super.receive 
}

class Passive1(t: ActorRef, q: BlockingQueue[Groom]) extends MockTC {

   override def preStart {
     super.preStart
     queue = Option(q)
     tester = Option(t)
   }

   override def received(d: Directive) = d.action match { 
     case Launch => doLaunch(d)
     case Cancel => doCancel(d)
     case Resume =>
   }

}

class Passive2(t: ActorRef, q: BlockingQueue[Groom]) extends MockTC {

   override def preStart {
     super.preStart
     queue = Option(q)
     tester = Option(t)
   }

   override def received(d: Directive) = d.action match { 
     case Launch => doLaunch(d)
     case Cancel => doCancel(d)
     case Resume =>
   }
}

class Passive3(t: ActorRef, q: BlockingQueue[Groom]) extends MockTC {

   override def preStart {
     super.preStart
     queue = Option(q)
     tester = Option(t)
   }

   override def received(d: Directive) = d.action match { 
     case Launch => doLaunch(d)
     case Cancel => doCancel(d)
     case Resume =>
   }
}

class Passive4(t: ActorRef, q: BlockingQueue[Groom]) extends MockTC {

   override def preStart {
     super.preStart
     queue = Option(q)
     tester = Option(t)
   }

   override def received(d: Directive) = d.action match { 
     case Launch => doLaunch(d)
     case Cancel => doCancel(d)
     case Resume =>
   }
}

class Passive5(t: ActorRef, q: BlockingQueue[Groom]) extends MockTC {

   override def preStart {
     super.preStart
     queue = Option(q)
     tester = Option(t)
   }

   override def received(d: Directive) = d.action match { 
     case Launch => doLaunch(d)
     case Cancel => doCancel(d)
     case Resume =>
   }
}

class Active1(t: ActorRef, q: BlockingQueue[Groom]) extends MockTC {

   override def preStart {
     super.preStart
     queue = Option(q)
     tester = Option(t)
   }

   override def received(d: Directive) = d.action match { 
     case Launch => doLaunch(d)
     case Cancel => doCancel(d)
     case Resume =>
   }
}

class Active2(t: ActorRef, q: BlockingQueue[Groom]) extends MockTC {

   override def preStart {
     super.preStart
     queue = Option(q)
     tester = Option(t)
   }

   override def received(d: Directive) = d.action match { 
     case Launch => doLaunch(d)
     case Cancel => doCancel(d)
     case Resume =>
   }
}

class Active3(t: ActorRef, q: BlockingQueue[Groom]) extends MockTC {

   override def preStart {
     super.preStart
     queue = Option(q)
     tester = Option(t)
   }

   override def received(d: Directive) = d.action match { 
     case Launch => doLaunch(d)
     case Cancel => doCancel(d)
     case Resume =>
   }
}

class Client(tester: ActorRef) extends Mock

class Master(actives: Array[ActorRef], passives: Array[ActorRef]) extends Mock {

  var scheduler: Option[ActorRef] = None

  var GroomName: String = "" 
  
  override def preStart = watch(actives, passives)

  def watch(actives: Array[ActorRef], passives: Array[ActorRef]) {
    actives.foreach(active => context watch active)
    passives.foreach(passive => context watch passive)
    LOG.info("Watch actives grooms: {} passives grooms: {}", 
             actives.map {a => a.path.name }.toArray.mkString("<", ", ", ">"), 
             passives.map { p=> p.path.name }.toArray.mkString("<", ", ", ">"))
  }

  def testMsg: Receive = {
    case GetTargetRefs(infos) => {
      LOG.info("{} asks for active grooms {}!", sender.path.name, 
               actives.map { a => a.path.name }.mkString(", "))
      sender ! TargetRefs(actives)
    }
    case Refs(sched, null) => {
      LOG.debug("Dispatch {} to active and passive grooms!", sched.path.name)
      this.scheduler = Option(sched)
      actives.foreach { active => active ! Refs(sched, self) }
      passives.foreach { passive => passive ! Refs(sched, self) }
    }
    case CalculateGroomTaskSize => {
      actives.foreach { active => active forward CalculateGroomTaskSize }
      passives.foreach { passive => passive forward CalculateGroomTaskSize }
    }
    case OfflinePassive(n) => {
      GroomName = "passive" + n
      LOG.info("########## Start groom {} offline event! ##########", GroomName)
      passives.find(passive => passive.path.name.equals(GroomName)) match {
        case Some(ref) => {
          LOG.info("{} is notified to offline!!!", ref.path.name)
          ref ! Close 
        }
        case None => LOG.error("No matched groom to offline: "+GroomName)
      }
    } 
    case Terminated(ref) => scheduler.map { sched => 
      if(ref.path.name.equals(GroomName)) 
        sched ! GroomLeave(GroomName, GroomName, 50000)
      else LOG.error("{} notifies actor {} is offline!", ref.path.name, 
                     sender.path.name)  
    }
    case FindGroomsToRestartTasks(infos) => {
      val (matched, nomatched) = findGroomsBy(infos)
      LOG.info("Grooms to restart => matched: {} nomatched: {}", matched,
               nomatched)
      sender ! GroomsToRestartFound(matched, nomatched)
    }
  }

  protected def findGroomsBy(infos: Set[SystemInfo]): 
      (Set[ActorRef], Set[String]) = {
    val activeGroom = true
    val passiveGroom = false

    var matched = Set.empty[ActorRef]
    var nomatched = Set.empty[String]

    val group = infos.groupBy { info => info.getHost.startsWith("active") }

    group.get(activeGroom).map { infos => infos.foreach( info =>
      actives.find( active => active.path.name.equals(info.getHost)).
              map { ref => matched += ref }
    )}

    group.get(passiveGroom).map { infos => infos.foreach( info => 
      passives.find( passive => passive.path.name.equals(info.getHost)).
               map { ref => matched += ref }
    )}

    LOG.debug("Matched grooms: {}, infos: {}", matched, infos)

    if(matched.size != infos.size) 
      throw new RuntimeException("Expect "+infos.size+" grooms, but "+
                                 matched.size+" found!")

    (matched, nomatched)
  }


  override def receive = testMsg orElse super.receive 
}

object F {

  val latestCheckpoint: Long = 19241

}

class F(tester: ActorRef) extends Mock { // federator

  import F._

  def testMsg: Receive = {
    case AskFor(recepiant: String, action: Any) => action match {
      case GetGroomCapacity(grooms: Array[ActorRef]) => {
        val capacity = GroomCapacity(grooms.map{ groom => (groom -> 3) }.toMap)
        LOG.info("Make-up grooms capacity {}", capacity)
        sender ! capacity
      }
      case FindLatestCheckpoint(jobId) => {
        tester ! JobId(jobId.toString, sender.path.name)
        sender ! LatestCheckpoint(jobId, latestCheckpoint)
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

  import F._

  override def initializeServices { 
    super.initializeServices
    LOG.debug("Dispatch {} reference to grooms!", self.path.name)
    master ! Refs(self, null)
  }

  override def onlyPassiveTasksFail(host: String, port: Int, ticket: Ticket,
                                    failedTasks: java.util.List[Task]) {
    super.onlyPassiveTasksFail(host, port, ticket, failedTasks)
    jobManager.findTicketById(ticket.job.getId) match {
      case (s: Some[Stage], t: Some[Ticket]) => {
        val jobId = t.get.job.getId.toString
        val jobState = t.get.job.getState.toString
        val tasksState = failedTasks.map { task => task.getState.toString }.
                                     toArray.mkString("<", ", ", ">")
        LOG.info("Current job {} is at state {}, tasks state {}", jobId, 
                 jobState, tasksState)
        tester ! CurrentState(jobId, jobState, tasksState)
      }
      case _ => throw new RuntimeException("Can't find ticket with job id "+
                                           ticket.job.getId)
    }
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

  override def whenAllTasksAssigned(ticket: Ticket): Boolean = 
    super.whenAllTasksAssigned(ticket) match {
      case true => {
        LOG.info("Inform master to calculate groom task size mapping ...")
        master ! CalculateGroomTaskSize 
        true
      }
      case false => false
    }  

/*
  override def whenAllTasksStopped(ticket: Ticket) {
    super.whenAllTasksStopped(ticket)
  }
*/

  override def updateJob(jobId: BSPJobID, latest: Long): Job = {
    val updated = super.updateJob(jobId, latest)
    LOG.debug("Job after updated: {}", updated)
    val updatedJob = UpdatedJob(updated.getId, updated.getName, 
                             updated.getMaxTaskAttempts, "RESTARTING",
                             latestCheckpoint, 
                             updated.targetGrooms.mkString(","))
    LOG.info("Extracted job information: {}", updatedJob)
    tester ! updatedJob
    val tasksList = updated.allTasks.map { task => {
      val updatedTask = UpdatedTask(task.getId, task.isAssigned, 
                                    task.getAssignedHost, task.getAssignedPort,
                                    task.getTotalBSPTasks)
      updatedTask
    }}.toList
    LOG.info("Extracted tasks information: {}", tasksList)
    tester ! tasksList
    updated
  }

  override def receive = super.receive

}

@RunWith(classOf[JUnitRunner])
class TestScheduler extends TestEnv("TestScheduler") with JobUtil {

  import F._

  val expectedTaskSize = 8
  val passiveTaskSize = 5
  val queue = new LinkedBlockingQueue[Groom]()
  var startGate = new CountDownLatch(1)
  //var cancelGate = new CountDownLatch(1)
  var taskMapping = Map.empty[String, Int]
  var executor: Option[ExecutorService] = None

  val jobName = "test-sched"
  val activeGrooms = Array("active3:50000", "avtive1:50000", "active2:50000") 

  val rand = new java.util.Random
  val emptySplit: PartitionedSplit = null
  val active: Boolean = true
  val passive: Boolean = false
  val assigned: Boolean = true
  val completed: Boolean = false

  val actives: Array[ActorRef] = Array(
    createWithArgs("active1", classOf[Active1], tester, queue),
    createWithArgs("active2", classOf[Active2], tester, queue), 
    createWithArgs("active3", classOf[Active3], tester, queue)
  )

  val passives: Array[ActorRef] = Array(
    createWithArgs("passive1", classOf[Passive1], tester, queue),
    createWithArgs("passive2", classOf[Passive2], tester, queue), 
    createWithArgs("passive3", classOf[Passive3], tester, queue),
    createWithArgs("passive4", classOf[Passive4], tester, queue),
    createWithArgs("passive5", classOf[Passive5], tester, queue)
  )

  class TaskSizeReceiver(startGate: CountDownLatch, 
                         q: BlockingQueue[Groom]) extends Callable[Boolean] {

    @throws(classOf[Exception])
    override def call(): Boolean = {
      while(!Thread.currentThread.isInterrupted) {
        val groom = q.take   
        LOG.info("Mock groom {} has {} tasks.", groom.name, groom.taskSize) 
        taskMapping += (groom.name -> groom.taskSize)
        if(expectedTaskSize == taskMapping.size) startGate.countDown
      }
      true
    }
  }

  override def beforeAll {
    super.beforeAll
    executor = Option(Executors.newSingleThreadExecutor)
    executor.map { exec => exec.submit(new TaskSizeReceiver(startGate, queue)) }
  }

  override def afterAll {
    executor.map {exec => exec.shutdown }
    super.afterAll
  } 
  def r(action: Action, id: TaskAttemptID, activeOrPassive: Boolean): Received =
    Received(action, id, 0, 0, emptySplit, 0, WAITING, SETUP, completed, 
             activeOrPassive, assigned, 8)

  def jobWithActiveGrooms(ident: String, id: Int): Job = createJob(ident, id, 
    jobName, activeGrooms, 8)

  def taskAttemptId(jobId: BSPJobID, taskId: Int, attemptId: Int): 
    TaskAttemptID = createTaskAttemptId(jobId, taskId, attemptId)

  def pickup: Int = rand.nextInt(passiveTaskSize) + 1 // 5 passive server

  def mkTasksState(size: Int, state: String): String = 
    (for(idx <- 0 until size) yield state).toArray.mkString("<", ", ", ">")

  def getTaskSize(num: Int): Int = taskMapping.get("passive"+num).getOrElse(-1)

  def expectedJob(old: Job): UpdatedJob =
    UpdatedJob(old.getId, jobName, old.getMaxTaskAttempts, "RESTARTING",
               latestCheckpoint, old.targetGrooms.mkString(","))

  def expectedTasks(ids: TaskAttemptID*): List[UpdatedTask] = ids.map { id =>
    UpdatedTask(id.next, false, SystemInfo.Localhost, 50000, expectedTaskSize)
  }.toList

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
    val federator = createWithArgs("mockFederator", classOf[F], tester)
    val master = createWithArgs("mockMaster", classOf[Master], actives, 
                                passives)
    val sched = createWithArgs("mockSched", classOf[MockScheduler], setting, 
                               master, receptionist, federator, 
                               tester)

    val r1 = r(Launch, taskAttemptId1, active)
    val r2 = r(Launch, taskAttemptId2, active)
    val r3 = r(Launch, taskAttemptId3, active)

    val r4 = r(Launch, taskAttemptId4, passive)
    val r5 = r(Launch, taskAttemptId5, passive)
    val r6 = r(Launch, taskAttemptId6, passive)
    val r7 = r(Launch, taskAttemptId7, passive)
    val r8 = r(Launch, taskAttemptId8, passive)

    LOG.info("Waiting assigned/ scheduled task size to be calculated ...")

    Try(startGate.await) match {
      case Success(ok) => LOG.info("Task size mapping => {}", taskMapping)
      case Failure(cause) => throw cause
    }

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
   
    var num = pickup 
    var taskSize = getTaskSize(num)
    
    while(0 >= taskSize) { 
      num = pickup
      taskSize = getTaskSize(num); 
    }

    LOG.info("Going to trigger offline groom 'passive{}' event where the "+
             " target groom has {} tasks!", num, taskSize)
    
    assert(0 < taskSize)
    // assert num value is between 1 - 5 (inclusive) as passive server.
    assert( 0 < num && passiveTaskSize >= num) 
    master ! OfflinePassive(num)

    expect(CurrentState(job.getId.toString, "RESTARTING", 
                        mkTasksState(taskSize, "FAILED")))

    // verify that cancel action is attempting to cancel 7 tasks (1 task fails)
    // with attempt id value set 1 
    expect(AttemptId(1))
    expect(AttemptId(1))
    expect(AttemptId(1))
    expect(AttemptId(1))
    expect(AttemptId(1))
    expect(AttemptId(1))
    expect(AttemptId(1))

    // find latest checkpoint from federator
    expect(JobId(job.getId.toString, sched.path.name))

    // verify job after updateJob function
    expect(expectedJob(job))

    // verify tasks after updatedJob function
    expectAnyOf(expectedTasks(taskAttemptId1, taskAttemptId2, taskAttemptId3, 
                              taskAttemptId4, taskAttemptId5, taskAttemptId6, 
                              taskAttemptId7, taskAttemptId8))

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
