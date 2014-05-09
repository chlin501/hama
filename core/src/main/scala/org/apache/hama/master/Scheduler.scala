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
import akka.actor.Cancellable
import org.apache.hama.bsp.v2.Job
import org.apache.hama.bsp.v2.Task
import org.apache.hama.bsp.v2.GroomServerStat
import org.apache.hama.groom.RequestTask
import org.apache.hama.HamaConfiguration
import org.apache.hama.LocalService
import org.apache.hama.master.Directive.Action._
import org.apache.hama.master.monitor.AskGroomServerStat
import org.apache.hama.ProxyInfo
import org.apache.hama.RemoteService
import org.apache.hama.Request
import scala.collection.immutable.Queue
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

final case object NextPlease

/**
 * - Pull a job from {@link Receptionist#waitQueue} if taskAssignQueue is empty.
 * - When scheduling a job, either active or passive, in 
 *   {@link #taskAssignQueue}, scheduler must check the number of total tasks 
 *   that belong to the same job can't exceed a GroomServer's maxTasks.
 *   EX: 
 *     task1 , task2 , task3 , task4 , ..., taskN    <-- task(s)
 *   [ groom1, groom2, groom1, groom8, ..., groom1 ] <-- run on GroomServer(s)
 *   if groom1's maxTasks is 2
 *   we observe tasks runnning on groom1 have count 3 (task1, task3, and taskN)
 *   so it's illegal/ wrong to schedule more than 2 tasks to groom1.
 *   only (max) 2 tasks are allowed to be scheduled groom1.
 */
class Scheduler(conf: HamaConfiguration) extends LocalService 
                                         with RemoteService {

  type GroomServerName = String
  type TaskManagerRef = ActorRef
  type MaxTasksAllowed = Int
  type TaskAssignQueue = Queue[Job]
  type ProcessingQueue = Queue[Job]

  val taskManagerInfo = new ProxyInfo.Builder().withConfiguration(conf).
                                                withActorName("taskManager").
                                                appendRootPath("groomServer").
                                                appendChildPath("taskManager").
                                                buildProxyAtGroom

  /**
   * A queue that holds jobs with tasks left unassigning to GroomServers.
   */
  protected var taskAssignQueue = Queue[Job]()

  /**
   * A queue that holds jobs having all tasks assigned to GroomServers.
   */
  protected var processingQueue = Queue[Job]()

  /** 
   * This map holds GroomServer name as key to its TaskManager Reference and
   * maxTasks value. 
   */
  protected var groomTaskManagers = 
    Map.empty[GroomServerName, (TaskManagerRef, MaxTasksAllowed)]

  var taskAssignQueueChecker: Cancellable = _

  override def configuration: HamaConfiguration = conf

  override def name: String = "sched"

  override def afterMediatorUp {
    taskAssignQueueChecker = request(self, NextPlease) 
  }

  def isTaskAssignQueueEmpty: Boolean = taskAssignQueue.isEmpty

  def request(target: ActorRef, message: Any): Cancellable = {
    import context.dispatcher
    context.system.scheduler.schedule(0.seconds, 3.seconds, target, message)
  }

  /**
   * Check if the taskQueue is empty. If true, ask Receptionist to dispense a 
   * job; otherwise do nothing.
   */
  def nextPlease: Receive = {
    case NextPlease => {
      if(isTaskAssignQueueEmpty) {
        if(null == mediator) 
          throw new IllegalStateException("Mediator shouldn't be null!")
        mediator ! Request("receptionist", TakeFromWaitQueue)
      }
    }
  }

  /**
   * Move a job to a specific queue pending for further processing
   *
   * If a job contains particular target GroomServer, schedule tasks to those
   * GroomServers.
   * 
   * Assuming GroomServer's maxTasks is not changed over time.
   */
  def dispense: Receive = {
    case Dispense(job) => { 
      taskAssignQueue = taskAssignQueue.enqueue(job)
      activeSchedule(job)    
    }
  }

  /**
   * Active schedule tasks within a job to particular GroomServers.
   * Tasks scheduled will be placed in target GroomServer's queue if free slots
   * are not available.
   * @param job contains tasks to be scheduled.
   */
  def activeSchedule(job: Job) {
    if(null != job.getTargets && 0 < job.getTargets.length) { 
      val (from, to) = schedule(taskAssignQueue)  
      this.taskAssignQueue = from
      this.processingQueue = processingQueue.enqueue(to.dequeue._1)
    }
  }

  /**
   * Positive schedule tasks.
   * Actual function that exhaustively schedules tasks to target GroomServers.
   */
  def schedule(fromQueue: TaskAssignQueue): 
      (TaskAssignQueue, ProcessingQueue) = { 
    LOG.info("TaskAssignQueue size is {}", fromQueue.size)
    val (job, rest) = fromQueue.dequeue
    val groomServers = job.getTargets  
    var from = Queue[Job](); var to = Queue[Job]()
    groomServers.foreach( groomName => {
      LOG.info("Check if groomTaskManagers cache contains {}", groomName)
      val (taskManagerActor, maxTasksAllowed) = 
        groomTaskManagers.getOrElse(groomName, (null, 0)) 
      if(null != taskManagerActor) {
        LOG.debug("GroomServer's taskManager {} found!", groomName)
        val currentTaskCount = job.getTaskCountFor(groomName)
        if(maxTasksAllowed < currentTaskCount)
          throw new IllegalStateException("Current tasks "+currentTaskCount+
                                          " for "+groomName+" exceeds "+
                                          maxTasksAllowed)
        if((currentTaskCount+1) <= maxTasksAllowed)  
          to = bookThenDispatch(job, taskManagerActor, groomName, dispatch)
        else throw new RuntimeException("Can't assign task because currently "+
                                        currentTaskCount+" tasks assigned "+
                                        "to groom server "+groomName+", "+
                                        "which allows "+maxTasksAllowed+
                                        " tasks to run.")
      } else LOG.warning("Can't find taskManager for {}", groomName)
    })
    if(!to.isEmpty) from = rest 
    LOG.debug("from queue: {} to queue: {}", from, to)
    (from, to)
  }

  /* && job.tasks if exceed maxTasks */ 
/* validate here or in receptionist?
  def validate(targetGroomServers: Array[String], 
               groomServer: String, maxTasks: Int): Boolean = {
    val map = targetGroomServers.groupBy(key=>key).mapValues{ ary=>ary.size} 
    val tasksSum = map.getOrElse(groomServer, -1)
    var flag = false
    if(-1 != tasksSum && tasksSum < maxTasks) {
      flag = true
    } 
    flag
  }
*/

  /**
   * Mark the task with the {@link GroomServer} requested; then dispatch that 
   * task to that {@link GroomServer}.
   * @param job contains tasks to be scheduled.
   * @param targetActor is the remote GroomServer's {@link TaskManager}.
   * @param targetGroomServer to which the task will be scheduled.
   * @param d is the dispatch function.
   */
  def bookThenDispatch(job: Job, targetActor: TaskManagerRef,  
                       targetGroomServer: String, 
                       d: (TaskManagerRef, Task) => Unit): ProcessingQueue = {
    var to = Queue[Job]()
    unassignedTask(job) match {
      case Some(task) => {
        // scan job's tasks checking if sumup of scheduled to the same groom 
        // server's tasks > maxTasks if passive assigned.
        task.markWithTarget(targetGroomServer) 
        d(targetActor, task)
      }
      case None => 
    }
    LOG.debug("Are all tasks assigned? {}", job.areAllTasksAssigned)
    if(job.areAllTasksAssigned) to = to.enqueue(job)
    to
  }

  /** 
   * Dispatch a Task to a GroomServer.
   * @param from is the GroomServer task manager.
   * @param task is the task to be executed.
   */
  protected def dispatch(from: TaskManagerRef, task: Task) {
    from ! new Directive(Launch, task,  
                         conf.get("bsp.master.name", "bspmaster"))  
  }

  /**
   * Dispense next unassigned task. None indiecates all tasks are assigned.
   */
  def unassignedTask(job: Job): Option[Task] = {
    val task = job.nextUnassignedTask;
    if(null != task) Some(task) else None
  }

  /**
   * From GroomManager to notify a groom server's task manager is ready for
   * receiving tasks dispatch.
   */
  def enrollment: Receive = {
    case GroomEnrollment(groomServerName, taskManager, maxTasks) => {
      groomTaskManagers ++= Map(groomServerName -> (taskManager, maxTasks)) 
    }
  }

  /**
   * GroomServer request for assigning a task.
   */
  def requestTask: Receive = {
    case RequestTask(groomServerStat) => {
      LOG.info("GroomServer {} requests task assign.", groomServerStat)
    }
  }

/*

  def passiveAssign(fromActor: ActorRef) {
      val (from, to) = 
        assign(groomServerName, taskAssignQueue, fromActor, dispatch) 
      this.taskAssignQueue = from
      if(!to.isEmpty) 
        this.processingQueue = processingQueue.enqueue(to.dequeue._1)
  }

   * Assign a task to a particular GroomServer by delegating that task to 
   * dispatch function, which normally uses actor ! message.
  def assign(fromGroom: String, fromQueue: TaskAssignQueue, 
             fromActor: ActorRef, d: (ActorRef, Task) => Unit): 
      (TaskAssignQueue, ProcessingQueue) = {
    if(!fromQueue.isEmpty) {
      val (job, rest) = fromQueue.dequeue 
      var from = Queue[Job]()
      val to = bookThenDispatch(job, fromActor, fromGroom, d) 
      if(!to.isEmpty) from = rest
      LOG.debug("from queue: {}, to queue: {}", from, to)
      (from, to)
    } else {
      (Queue[Job](), Queue[Job]())
    }
  }
  
  * Rescheduling tasks when a GroomServer goes offline.
  def reschedTasks: Receive = {
    case RescheduleTasks(spec) => {
       LOG.info("Failed GroomServer having GroomServerSpec "+spec)
       // TODO: 1. check if job.getTargets is empty or not.
       //       2. if targets has values, call schedule(); otherwise 
       //          put into assign task queue, waiting for groom request.
    }
  }

  def lookupTaskManager(spec: GroomServerSpec) {
    LOG.info("Lookup {} at {}", spec.getName, taskManagerInfo.getPath)
    lookup(spec.getName, taskManagerInfo.getPath)
  }

  from GroomManager for looking up registered TaskManager at GroomServer.
  def locate: Receive = {
    case Locate(spec) => {
      lookupTaskManager(spec)
    }
  }

  override def receive = locate orElse reschedTasks orElse jobSubmission orElse dispense orElse requestTask orElse 
*/
  override def receive = requestTask orElse dispense orElse nextPlease orElse enrollment orElse isServiceReady orElse mediatorIsUp orElse isProxyReady orElse timeout orElse unknown
}
