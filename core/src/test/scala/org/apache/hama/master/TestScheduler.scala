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
import akka.actor.Terminated
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.apache.hama.Close
import org.apache.hama.HamaConfiguration
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
import org.apache.hama.groom.TaskFailure
import org.apache.hama.groom.TaskRequest
import org.apache.hama.groom.RequestTask
import org.apache.hama.monitor.GroomStats
import org.apache.hama.monitor.SlotStats
import org.apache.hama.monitor.master.GetGroomCapacity
import org.apache.hama.monitor.master.GroomCapacity
import org.apache.hama.util.JobUtil
import org.apache.hama.util.Utils._
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
final case class OfflineActive(n: Int)
final case class OfflinePassive(n: Int)
final case class FailTask(groomId: Int, passive: Boolean = true)
final case class CurrentState(jobId: String, jobState: String,
                              tasksState: String)
final case class Groom(name: String, taskSize: Int)
final case class AttemptId(id: Int)
final case class JobId(jobId: String, from: String)
final case class UpdatedJob(jobId: BSPJobID, jobName: String, 
                            maxTaskAttempts: Int, state: String,
                            latestCheckpoint: Long, activeGrooms: String)
final case class UpdatedTasks(jobId: TaskAttemptID, assigned: Boolean,
                              assignedHost: String, assignedPort: Int,
                              totalBSPTasks: Int)
final case class TaskAttemptIds(ids: Array[TaskAttemptID])
final case class Error(reason: String)
final case object Await
final case object TaskSize
final case object TaskMapping

trait MockTC extends Mock with Periodically {// task counsellor

  var scheduler: Option[ActorRef] = None

  var master: Option[ActorRef] = None

  var tester: Option[ActorRef] = None

  var tasks = Array.empty[Task]  
 
  var resumeCount = 0
  var launchCount = 0

  def tasksLength(): Int = tasks.size

  override def preStart = tick(self, TaskRequest, delay = 1.seconds)

  def testMsg: Receive = {
    case Refs(sched, m) => {
      this.scheduler = Option(sched)
      this.master = Option(m)
    }
    case directive: Directive => directive match {
      case null => throw new RuntimeException("Directive shouldn't be null!")
      case d@_ => received(d)
    }
    case FailTask(id, isPassive) => if(0 < tasksLength) {
      LOG.debug("Currently {} has tasks {}", name, tasks.mkString(","))
      val failed = tasks.head
      remove(failed)
      scheduler.map { sched => 
        LOG.debug("Notify scheduler task {} fails ...", failed.getId)
        sched ! TaskFailure(failed.getId, currentGroomStats) 
      } 
    } else tester.map { t => { 
      LOG.debug("Expect 0 task! Currently {} has tasks {}.", name, 
                tasks.mkString(","))
      t ! Error("No task exists at groom "+name+"!")
    }}
    case TaskSize => sender ! tasksLength
  }

  override def ticked(msg: Tick): Unit = msg match {
    case TaskRequest => requestNewTask()
  }

  def received(d: Directive) = {
    LOG.debug("Groom {} receives action {} task {}!!! Total tasks now: {}", name, d.action, d.task.getId, tasks.mkString("<", ",", ">"))
    d.action match { 
      case Launch => doLaunch(d)
      case Cancel => doCancel(d)
      case Resume => doResume(d)
    }
  }

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
    LOG.debug("[before remove] going to remove task {} at {}", task.getId, 
              name)
    tasks = tasks diff Array(task)
    LOG.debug("[after remove] {} has {} tasks: {}", name, tasks.length, 
              tasks.mkString(", "))
  }

  def doLaunch(d: Directive) {
    launchCount += 1
    val r = c(d)
    LOG.debug("Received to be verified: {}", r)
    tester.map { t => t ! r }
    add(d.task)     
    LOG.debug("Directive at {} now has {} tasks!", name, tasksLength)
  }

  def doCancel(d: Directive) {
    d.task.getConfiguration.getBoolean("test.task.failure", false) match {
      case true => tester.map { t => t ! d.task.getId }
      case false => tester.map { t => t ! AttemptId(d.task.getId.getId) }
    }
    scheduler.map { sched => sched ! TaskCancelled(d.task.getId.toString) }
    remove(d.task)
    LOG.debug("Task {} now is canceled by {}", d.task.getId, name)
  }

  def doResume(d: Directive) { 
    resumeCount += 1
    add(d.task)     
  }

  override def receive = testMsg orElse tickMessage orElse super.receive 
}

class Passive1(t: ActorRef) extends MockTC {

   override def preStart {
     super.preStart
     tester = Option(t)
   }
}

class Passive2(t: ActorRef) extends MockTC {

   override def preStart {
     super.preStart
     tester = Option(t)
   }

}

class Passive3(t: ActorRef) extends MockTC {

   override def preStart {
     super.preStart
     tester = Option(t)
   }

}

class Passive4(t: ActorRef) extends MockTC {

   override def preStart {
     super.preStart
     tester = Option(t)
   }

}

class Passive5(t: ActorRef) extends MockTC {

   override def preStart {
     super.preStart
     tester = Option(t)
   }

}

class Active1(t: ActorRef) extends MockTC {

   override def preStart {
     super.preStart
     tester = Option(t)
   }
}

class Active2(t: ActorRef) extends MockTC {

   override def preStart {
     super.preStart
     tester = Option(t)
   }
}

class Active3(t: ActorRef) extends MockTC {

   override def preStart {
     super.preStart
     tester = Option(t)
   }
}

class Client(tester: ActorRef) extends Mock

class Master(actives: Array[ActorRef], passives: Array[ActorRef], 
             tester: ActorRef) extends Mock {

  var scheduler: Option[ActorRef] = None

  var GroomName: String = "" 
  
  var nCount = 0

  override def preStart = watch(actives, passives)

  def watch(actives: Array[ActorRef], passives: Array[ActorRef]) {
    actives.foreach(active => context watch active)
    passives.foreach(passive => context watch passive)
    LOG.debug("Watch actives grooms: {} passives grooms: {}", 
             actives.map {a => a.path.name }.toArray.mkString("<", ", ", ">"), 
             passives.map { p=> p.path.name }.toArray.mkString("<", ", ", ">"))
  }

  def testMsg: Receive = {
    case GetTargetRefs(infos) => {
      LOG.debug("{} asks for active grooms {}!", sender.path.name, 
               actives.map { a => a.path.name }.mkString(", "))
      sender ! TargetRefs(actives)
    }
    case Refs(sched, null) => {
      LOG.debug("Dispatch {} to active and passive grooms!", sched.path.name)
      this.scheduler = Option(sched)
      actives.foreach { active => active ! Refs(sched, self) }
      passives.foreach { passive => passive ! Refs(sched, self) }
    }
    case FailTask(id, isPassive) => (isPassive match {
      case true => passives.filter { passive => passive.path.name.
        equals(("passive"+id)) }
      case false => actives.filter { active  => active.path.name.
        equals(("active"+id)) } }).head ! FailTask(id, isPassive)
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
      val (matched, nomatched) = findGroomsBy0(infos)
      LOG.info("Grooms to restart => matched: {} nomatched: {}", matched,
               nomatched)
      sender ! GroomsToRestartFound(matched, nomatched)
    }
    case FindTasksAliveGrooms(infos) => {
      val (matched, nomatched) = findGroomsBy0(infos)
      nomatched.isEmpty match {
        case true => sender ! TasksAliveGrooms(matched)
        case false => {
          tester ! Error("Can't find grooms "+nomatched.mkString(",") +
                         " for restarting tasks!")
        }
      }
    } 
  }

  def findPassiveBy(infos: Set[SystemInfo]):
      (Set[ActorRef], Set[String]) = {
    var matched = Set.empty[ActorRef]
    var nomatched = Set.empty[String]
    infos.foreach( info => passives.find( groom =>
      groom.path.address.host.equals(Option(info.getHost)) &&
      groom.path.address.port.equals(Option(info.getPort))
    ) match {
      case Some(ref) => matched += ref
      case None => nomatched += info.getHost+":"+info.getPort
    })
    (matched, nomatched)
  }

  def findGroomsBy0(infos: Set[SystemInfo]): (Set[ActorRef], Set[String]) = {
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

  val ckpt: Seq[Long] = Seq(11, 13, 17, 19, 23, 29, 51, 53, 57)

}

class F(tester: ActorRef) extends Mock { // federator

  var pos = 0

  import F._

  def testMsg: Receive = {
    case AskFor(recepiant: String, action: Any) => action match {
      case GetGroomCapacity(grooms: Array[ActorRef]) => {
        val capacity = GroomCapacity(grooms.map{ groom => (groom -> 3) }.toMap)
        LOG.debug("Artifical grooms capacity: {}", capacity)
        sender ! capacity
      }
      case FindLatestCheckpoint(jobId) => {
        val latestCheckpoint = ckpt(pos)
        pos += 1
        //LOG.debug("{} asks the latest checkpoint, which is {}, for job {}!", sender.path.name, latestCheckpoint, jobId)
        //tester ! JobId(jobId.toString, sender.path.name)
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

  var taskMapping = Map.empty[String, Int]
  var testTaskFailure = false

  var pos = 0

  import F._

  override def initializeServices { 
    super.initializeServices
    LOG.debug("Dispatch {} reference to grooms!", self.path.name)
    master ! Refs(self, null)
  }

  override def afterAllTasksAssigned(ticket: Ticket) {
    LOG.info("Task mapping now: {} ", taskMapping)
  }
 
  override def afterPassiveAssignTask(from: ActorRef, task: Task) = 
    taskMapping.find { case (k, v) => from.path.name.equals(k) } match {
      case Some((k, v)) => 
      case None => taskMapping += (from.path.name -> 1)
    }

  override def afterActiveScheduleTask(ref: ActorRef, task: Task) = 
    taskMapping.find { case (k, v) => ref.path.name.equals(k) } match {
      case Some((k, v)) => 
      case None => taskMapping += (ref.path.name -> 1)
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
      case true => true
      case false => false
    }  

  override def updateJob(jobId: BSPJobID, latest: Long): Job = {
    val updated = super.updateJob(jobId, latest)
    taskMapping = Map.empty[String, Int]
    val updatedJob = UpdatedJob(updated.getId, updated.getName, 
                             updated.getMaxTaskAttempts, "RESTARTING",
                             ckpt(pos), 
                             updated.targetGrooms.mkString(","))
    pos += 1
    LOG.info("Extracted job information: {}", updatedJob)
    tester ! updatedJob
    val tasksList = updated.allTasks.map { task => {
      val updatedTask = UpdatedTasks(task.getId, task.isAssigned, 
                                     task.getAssignedHost, task.getAssignedPort,
                                     task.getTotalBSPTasks)
      updatedTask
    }}.toList
    LOG.info("Extracted tasks information: {}", tasksList)
    tester ! tasksList
    updated
  }

  override def beforeCancelTask(groom: ActorRef, failed: Task): 
      (ActorRef, Task) = {
    if(testTaskFailure) {
      failed.getConfiguration.setBoolean("test.task.failure", true)  
      LOG.debug("Failed task {} with test.task.failure set to {}", 
                failed.getId, failed.getConfiguration.get("test.task.failure"))
    }
    (groom, failed)
  }

  override def afterCancelTask(groom: ActorRef, failed: Task) =
    if(testTaskFailure) testTaskFailure = false

  def testMsg: Receive = {
    case FailTask(id, isPassive) => {
      testTaskFailure = true
      master ! FailTask(id, isPassive)
    }
    case TaskMapping => LOG.debug("Curent task mapping {}", taskMapping)
  }

  override def receive = testMsg orElse super.receive

}

@RunWith(classOf[JUnitRunner])
class TestScheduler extends TestEnv("TestScheduler") with JobUtil {

  import F._

  val expectedTaskSize = 8
  val expectedGroomSize = 8
  val activeTaskSize = 3
  val passiveTaskSize = 5

  val jobName = "test-sched"
  val activeGrooms = Array("active3:50000", "avtive1:50000", "active2:50000") 

  val rand = new java.util.Random
  val emptySplit: PartitionedSplit = null
  val active: Boolean = true
  val passive: Boolean = false
  val assigned: Boolean = true
  val completed: Boolean = false
  val failPassiveTask = true
  val failActiveTask = false
  var pos = 0

  val active1 = createWithArgs("active1", classOf[Active1], tester)
  val active2 = createWithArgs("active2", classOf[Active2], tester) 
  val active3 = createWithArgs("active3", classOf[Active3], tester) 

  val passive1 = createWithArgs("passive1", classOf[Passive1], tester)
  val passive2 = createWithArgs("passive2", classOf[Passive2], tester) 
  val passive3 = createWithArgs("passive3", classOf[Passive3], tester) 
  val passive4 = createWithArgs("passive4", classOf[Passive4], tester) 
  val passive5 = createWithArgs("passive5", classOf[Passive5], tester) 

  var client: ActorRef = _
  var receptionist: ActorRef = _
  var sched: ActorRef = _
  var master: ActorRef = _
  var federator: ActorRef = _
  var job: Job = _

  def actives: Array[ActorRef] = Array(active1, active2, active3)

  def passives: Array[ActorRef] = Array(passive1, passive2, passive3, passive4, passive5)

  def taskAttemptIds(jobId: BSPJobID, size: Int, attempt: Int): 
    TaskAttemptIds = TaskAttemptIds((for(idx <- 1 to size) 
    yield taskAttemptId(jobId, idx, attempt)).toArray)

  def createUpdatedTasks(ids: Array[TaskAttemptID]): List[UpdatedTasks] = 
    ids.map { id => UpdatedTasks(id, false, SystemInfo.Localhost, 50000, 8) }.
        toList

  def r(action: Action, id: TaskAttemptID, activeOrPassive: Boolean): Received =
    Received(action, id, 0, 0, emptySplit, 0, WAITING, SETUP, completed, 
             activeOrPassive, assigned, 8)

  def jobWithActiveGrooms(ident: String, id: Int, 
                          conf: HamaConfiguration): Job = 
    createJob(ident, id, jobName, activeGrooms, 8).addConfiguration(conf)

  def taskAttemptId(jobId: BSPJobID, taskId: Int, attemptId: Int): 
    TaskAttemptID = createTaskAttemptId(jobId, taskId, attemptId)

  def pickup(n: Int): Int = rand.nextInt() + 1 

  def mkTasksState(size: Int, state: String): String = 
    (for(idx <- 0 until size) yield state).toArray.mkString("<", ", ", ">")

  def expectedJob(old: Job): UpdatedJob = {
    val u = UpdatedJob(old.getId, jobName, old.getMaxTaskAttempts, "RESTARTING",
               ckpt(pos), old.targetGrooms.mkString(","))
    pos += 1
    u
  }

  def expectedTasks(ids: TaskAttemptID*): List[UpdatedTasks] = ids.map { id =>
    UpdatedTasks(id.next, false, SystemInfo.Localhost, 50000, expectedTaskSize)
  }.toList

  def init() {
    val setting = Setting.master
    setting.hama.setInt("bsp.tasks.max.attempts", 10)
    job = jobWithActiveGrooms("test", 2, setting.hama)
    assert(10 == job.getConfiguration.getInt("bsp.tasks.max.attempts", -1))

    client = createWithArgs("mockClient", classOf[Client], tester)
    receptionist = createWithArgs("mockReceptionist", classOf[R], client)
    receptionist ! J(job)
    federator = createWithArgs("mockFederator", classOf[F], tester)
    master = createWithArgs("mockMaster", classOf[Master], actives, passives, tester)
    sched = createWithArgs("mockSched", classOf[MockScheduler], setting, master, receptionist, federator, tester)
  }

  def stopAll() {
    system.stop(client)
    system.stop(receptionist)
    system.stop(federator)
    system.stop(master)
    system.stop(sched)
  }

  it("test scheduling functions.") {

    init()

    LOG.info("Wait 3 secs before test launch messages ...")
    Thread.sleep(3*1000)

    sched ! TaskMapping

    LOG.info("Start testing to launch task ...")
    val ids1 = taskAttemptIds(job.getId, expectedTaskSize, 1).ids
    testLaunch(ids1)  
    LOG.info("Finiah testing to launch task ...")

    Thread.sleep(5*1000)

    LOG.info("Start testing passive single task failure ...") 
    sched ! FailTask(3, failPassiveTask) 
    val ids2 = taskAttemptIds(job.getId, expectedTaskSize, 2).ids
    val updatedTasks1 = createUpdatedTasks(ids2)
    testTaskFailure(ids1, updatedTasks1, job, sched) 
    LOG.info("Finish testing passive single task failure ...") 

    Thread.sleep(5*1000)

    LOG.info("Start testing active single task failure ...") 
    sched ! FailTask(2, failActiveTask) 
    val ids3 = taskAttemptIds(job.getId, expectedTaskSize, 3).ids
    val updatedTasks2 = createUpdatedTasks(ids3)
    testTaskFailure(ids2, updatedTasks2, job, sched) 
    LOG.info("Finish testing active single task failure ...") 

    Thread.sleep(10*1000)

    LOG.info("Start testing multiple passive tasks failure ...") 
    sched ! FailTask(4, failPassiveTask)  
    sched ! FailTask(1, failPassiveTask)  
    val ids4 = taskAttemptIds(job.getId, expectedTaskSize, 4).ids
    val updatedTasks3 = createUpdatedTasks(ids4)
    val multipleTasks = true
    testTaskFailure(ids3, updatedTasks3, job, sched, multipleTasks) 
    LOG.info("Finish testing multiple passive tasks failure ...") 
  


//    // single groom leave event
//    master ! OfflinePassive(values.id)
//
//    expect(CurrentState(job.getId.toString, "RESTARTING", 
//                        mkTasksState(taskSize, "FAILED")))
//
//    // verify that cancel action is attempting to cancel 7 tasks (1 task fails)
//    // with attempt id value set 1 
//    expect(AttemptId(1))
//    expect(AttemptId(1))
//    expect(AttemptId(1))
//    expect(AttemptId(1))
//    expect(AttemptId(1))
//    expect(AttemptId(1))
//    expect(AttemptId(1))
//
//    // find latest checkpoint from federator
//    expect(JobId(job.getId.toString, sched.path.name))
//
//    // verify job after updateJob function
//    expect(expectedJob(job))
//
//    // verify tasks after updatedJob function
//    expectAnyOf(expectedTasks(taskAttemptId1, taskAttemptId2, taskAttemptId3, 
//                              taskAttemptId4, taskAttemptId5, taskAttemptId6, 
//                              taskAttemptId7, taskAttemptId8))
//   
//    // all tasks are successfully dispatch to grooms and they are all resume
//    // action.
//    
//

    LOG.info("Done testing scheduler functions!")
  }

  def testLaunch(ids: Array[TaskAttemptID]) {
    val r1 = r(Launch, ids(0), active)
    val r2 = r(Launch, ids(1), active)
    val r3 = r(Launch, ids(2), active)

    val r4 = r(Launch, ids(3), passive)
    val r5 = r(Launch, ids(4), passive)
    val r6 = r(Launch, ids(5), passive)
    val r7 = r(Launch, ids(6), passive)
    val r8 = r(Launch, ids(7), passive)

    LOG.info("Expect Received messages after tasks are launched ...")
    expectAnyOf(r1, r2, r3, r4, r5, r6, r7, r8)
    expectAnyOf(r1, r2, r3, r4, r5, r6, r7, r8)
    expectAnyOf(r1, r2, r3, r4, r5, r6, r7, r8)
    expectAnyOf(r1, r2, r3, r4, r5, r6, r7, r8)
    expectAnyOf(r1, r2, r3, r4, r5, r6, r7, r8)
    expectAnyOf(r1, r2, r3, r4, r5, r6, r7, r8)
    expectAnyOf(r1, r2, r3, r4, r5, r6, r7, r8)
    expectAnyOf(r1, r2, r3, r4, r5, r6, r7, r8)
    LOG.info("All Received messages are verified ...")
  }
 
  def testTaskFailure(ids: Array[TaskAttemptID], 
                      updatedTasks: List[UpdatedTasks],
                      job: Job, sched: ActorRef, multiple: Boolean = false) {

    LOG.info("Wait 3 secs before testing task failure scenario ...")
    Thread.sleep(3*1000)

    LOG.info("Expect task attempt id ... ")
    // do cancel
    expectAnyOf(ids(0), ids(1), ids(2), ids(3), ids(4), ids(5), ids(6), ids(7)) 
    expectAnyOf(ids(0), ids(1), ids(2), ids(3), ids(4), ids(5), ids(6), ids(7)) 
    expectAnyOf(ids(0), ids(1), ids(2), ids(3), ids(4), ids(5), ids(6), ids(7)) 
    expectAnyOf(ids(0), ids(1), ids(2), ids(3), ids(4), ids(5), ids(6), ids(7)) 
    expectAnyOf(ids(0), ids(1), ids(2), ids(3), ids(4), ids(5), ids(6), ids(7)) 
    expectAnyOf(ids(0), ids(1), ids(2), ids(3), ids(4), ids(5), ids(6), ids(7)) 
    expectAnyOf(ids(0), ids(1), ids(2), ids(3), ids(4), ids(5), ids(6), ids(7)) 

    LOG.info("Expect updated job data ... ")
    expect(expectedJob(job))

    LOG.info("Expect updated task data ... ")
    expectAnyOf(updatedTasks)

  }

}
*/
