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
package org.apache.hama.groom

import akka.actor.ActorRef
import akka.actor.Cancellable
import akka.actor.Props
import org.apache.hama.bsp.v2.GroomServerStat
import org.apache.hama.bsp.v2.Task
import org.apache.hama.bsp.TaskAttemptID
import org.apache.hama.HamaConfiguration
import org.apache.hama.LocalService
import org.apache.hama.RemoteService
import org.apache.hama.ProxyInfo
import org.apache.hama.lang.Executor
import org.apache.hama.lang.Fork
import org.apache.hama.master._
import org.apache.hama.master.Directive._
import org.apache.hama.master.Directive.Action._
import scala.collection.immutable.Queue
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

class TaskManager(conf: HamaConfiguration) extends LocalService 
                                           with RemoteService {

  type ForkedChild = String

  val schedInfo =
    new ProxyInfo.Builder().withConfiguration(configuration).
                            withActorName("sched").
                            appendRootPath("bspmaster").
                            appendChildPath("sched").
                            buildProxyAtMaster

  val schedPath = schedInfo.getPath

  val groomManagerInfo =
    new ProxyInfo.Builder().withConfiguration(configuration).
                            withActorName("groomManager").
                            appendRootPath("bspmaster").
                            appendChildPath("groomManager").
                            buildProxyAtMaster

  val groomManagerPath = groomManagerInfo.getPath

  val groomServerHost = configuration.get("bsp.groom.address", "127.0.0.1")
  val groomServerPort = configuration.getInt("bsp.groom.port", 50000)
  val groomServerName = "groom_"+ groomServerHost +"_"+ groomServerPort
  val maxTasks = configuration.getInt("bsp.tasks.maximum", 3) 
  val bspmaster = configuration.get("bsp.master.name", "bspmaster")

  var sched: ActorRef = _
  var cancellable: Cancellable = _
  var groomManager: ActorRef = _

  var children = Map.empty[ForkedChild, ActorRef]

  /**
   * The max size of slots can't exceed configured maxTasks.
   */
  private var slots = Set.empty[Slot]

  /**
   * All {@link Directive}s are stored in this queue. 
   */
  private var directiveQueue = Queue[Directive]()

  /**
   * {@link Directive}s to be acked and then removed will be placed in this 
   * queue.
   */
  private var pendingQueue = Queue[Directive]()

  /* can be overriden in test. */
  protected def getGroomServerName(): String = groomServerName
  protected def getGroomServerHost(): String = groomServerHost
  protected def getGroomServerPort(): Int = groomServerPort
  protected def getMaxTasks(): Int = maxTasks
  protected def getSchedulerPath: String = schedPath

  override def configuration: HamaConfiguration = conf

  override def name: String = "taskManager"

  /**
   * Initialize slots with default slots value to 3, which comes from maxTasks,
   * or "bsp.tasks.maximum".
   * @param constraint of the slots can be created.
   */
  protected def initializeSlots(constraint: Int = 3) {
    for(seq <- 1 to constraint) {
      slots ++= Set(Slot(seq, None, bspmaster, None))
    }
    LOG.debug("{} GroomServer slots are initialied.", constraint)
  }

  override def initializeServices {
    initializeSlots(getMaxTasks)
    lookup("sched", schedPath)
    lookup("groomManager", groomManagerPath)
  }

  def hasTaskInQueue: Boolean = !directiveQueue.isEmpty

  /**
   * Check if there is slot available. 
   * @return Boolean denotes whether having free slots. Tree if free slots 
   *                 available, false otherwise.
   */
  def hasFreeSlots(): Boolean = {
    var isOccupied = true
    var hasFreeSlot = false
    slots.takeWhile(slot => {
      isOccupied = !None.equals(slot.task)
      isOccupied
    })
    if(!isOccupied) hasFreeSlot = true else hasFreeSlot = false
    hasFreeSlot
  }

  override def afterLinked(proxy: ActorRef) = {
    proxy.path.name match {
      case "sched" => {
        sched = proxy  
        cancellable = request(self, TaskRequest)
      } 
      case "groomManager" => { // register
        groomManager = proxy
        groomManager ! currentGroomServerStat
      } 
      case _ => LOG.info("Linking to an unknown proxy {}", proxy.path.name)
    }
  }

  override def offline(target: ActorRef) {
    // TODO: if only groomManager actor fails, simply re-"lookup" will fail.
    lookup("groomManager", groomManagerPath)
  }

  /**
   * Check if slots are available and any unprocessed directive in queue. When
   * slots are free but any directives exist, process directive first. 
   * @return Receive is partial function.
   */
  def taskRequest: Receive = {
    case TaskRequest => {
      LOG.debug("In TaskRequest, sched: {}, hasTaskInQueue: {}"+
               ", hasFreeSlots: {}", sched, hasTaskInQueue, hasFreeSlots)
      if(hasFreeSlots) { 
        if(!hasTaskInQueue) { 
          LOG.debug("Request {} for assigning new tasks ...", getSchedulerPath)
          sched ! RequestTask(currentGroomServerStat)
        } 
      } 
    }
  }
  
  /**
   * Calculate sys loading vlaue, including cpu, memory, etc.
  def sysload: Double = {
  }
   */

  /**
   * Collect tasks information for report.
   * @return GroomServerStat contains the latest tasks statistics.
   */
  def currentGroomServerStat(): GroomServerStat = {
    val stat = new GroomServerStat(getGroomServerName, getGroomServerHost, 
                                   getGroomServerPort, getMaxTasks)
    directiveQueue.foreach( directive => {
      directive match {
        case null => {
          LOG.warning("Directive shouldn't be null!")
          stat.addToQueue("(null)")
        }
        case _ => stat.addToQueue(directive.task.getId.toString)
      }
    })

    if(slots.size != stat.slotsLength)
      throw new RuntimeException("Incorrect slots size: "+stat.slotsLength);
    var pos = 0
    slots.foreach( slot => {
      slot.task match {
        case None => stat.markWithNull(pos)
        case Some(aTask) => stat.mark(pos, aTask.getId.toString)
      }
      pos += 1
    }) 
    stat
  } 

  def findTargetToKill(task: Task): Option[ActorRef] = { 
    slots.find(slot=> slot.task.equals(Some(task))) match {
      case Some(slot) => slot.executor 
      case None => None
    }
  }

  /**
   * 1. Create an Executor actor.
   * 2. Send Fork message so that {@link Executor} will fork a child process.
   * Forked child process will send {@link PullForExecution} message for task 
   * execution.
   */
  def initializeExecutor(master: String) {
    pickUp match {
      case Some(slot) => { 
        val executorName = configuration.get("bsp.groom.name", "groomServer") +
                           "_executor_" + slot.seq 
        // TODO: move to spawn()
        val executor = context.actorOf(Props(classOf[Executor], 
                                             configuration,
                                             self),
                                       executorName)  
        executor ! Fork(slot.seq, configuration) 
        val newSlot = Slot(slot.seq, None, master, Some(executor))
        slots -= slot
        slots += newSlot
      }
      case None => // all slots are in use 
    }
  }

  /**
   * Pick up a slot that is not in use.
   * @return Option[Slot] contains either a slot or None if no slot available.
   */
  def pickUp: Option[Slot] = {
    var free: Slot = null
    var flag = true
    slots.takeWhile( slot => {
      val isEmpty = None.equals(slot.task)
      if(isEmpty) {
        free = slot
        flag = false
      } else {
        if(null != free) flag = false
      }
      flag
    })
    if(null == free) None else Some(free)
  }

  /**
   * Receive {@link Directive} from Scheduler, deciding what to do next.
   * @return Receive is partial function.
   */
  def receiveDirective: Receive = {
    case directive: Directive => { 
      directive match {
        case null => LOG.warning("Directive dispatched from {} is null!", 
                                 sender.path.name)
        case _ => {
          directive.action match {
            case Launch => {
               initializeExecutor(directive.master) 
               directiveQueue = directiveQueue.enqueue(directive)
            }
            case Kill => {
              findTargetToKill(directive.task) match {
                case Some(executor) => {
                  executor ! KillTask(directive.task.getId)
                  pendingQueue = pendingQueue.enqueue(directive) 
                }
                case None => LOG.warning("Ask to Kill task {}, but no "+
                                         "corresponded executor found!", 
                                         directive.task.toString)
              }
            }
            case Resume => directiveQueue = directiveQueue.enqueue(directive)
            case d@_ => LOG.warning("Unknown directive {}", d)
          }
        }
      }
    }
  }

  /**
   * Book the slot with corresponded {@link Task} and {@link Executor} when
   * receiving LaunchAck and ResumeAck.
   * @param slotSeq indicates the <i>N</i>th slot.
   * @param task is the task being executed
   * @param executor is the executor that runs the task.
   */
  def book(slotSeq: Int, task: Task, executor: ActorRef) {
    slots.find(slot => (slotSeq == slot.seq)) match {
      case Some(slot) => {
        val newSlot = Slot(slot.seq, Some(task), slot.master, Some(executor))
        slots -= slot 
        slots += newSlot
      }
      case None => throw new RuntimeException("Slot with seq "+slotSeq+" not "+
                                              "found")
    }
  }

  /**
   * Executor ack for Launch action.
   * @return Receive is partial function.
   */
  def launchAck: Receive = {
    case LaunchAck(slotSeq, taskAttemptId) => doAck(slotSeq, taskAttemptId)
  }

  /**
   * Executor ack for Resume action.
   * @return Receive is partial function.
   */
  def resumeAck: Receive = {
    case ResumeAck(slotSeq, taskAttemptId) => doAck(slotSeq, taskAttemptId)
  }

  /**
   * Executor ack for Kill action.
   * Verify corresponded task is with Kill action and correct taskAttemptId.
   * @param Receive is partial function.
   */
  def killAck: Receive = {
    case KillAck(slotSeq, taskAttemptId) => {
      slots.find( slot => {
        val seqEquals = (slot.seq == slotSeq)  
        val idEquals = slot.task match {
          case Some(found) => found.getId.equals(taskAttemptId)
          case None => false
        }
        seqEquals && idEquals
      }) match {
        case Some(slot) => {
          val newSlot = Slot(slot.seq, None, slot.master, slot.executor)
          slots -= slot 
          slots += newSlot 
        } 
        case None => LOG.warning("Killed task {} not found.", taskAttemptId)
      }
    }
  }

  /**
   * Update slot information according to {@link Slot#seq} and 
   * {@link TaskAttemptID} acked by {@link Executor}.
   * This function:
   * - search directive in pendingQueue.
   * - update slot information with found directive content. 
   * @param slotSeq is the sequence number of slot.
   * @param taskAttemptId is the task attempt id executed at BSPPeerContainer.
   */
  def doAck(slotSeq: Int, taskAttemptId: TaskAttemptID) {
    if(!pendingQueue.isEmpty) {
      pendingQueue.find( directive =>
        taskAttemptId.equals(directive.task.getId) 
      ) match {
        case Some(directive) => book(slotSeq, directive.task, sender)
        case None => LOG.error("No pending directive for task {}, slot {} "+
                               "matches ack.", taskAttemptId, slotSeq)
      }
    } else LOG.warning("Pending queue is empty when slot {}, task {} ack!", 
                       slotSeq, taskAttemptId)
  }

  /**
   * Executor on behalf of BSPPeerContainer requests for task execution.
   * - dequeue a directive from queue.
   * @return Receive is partial function.
   */
  def pullForExecution: Receive = {
    case PullForExecution(slotSeq) => {
      val (directive, rest) = directiveQueue.dequeue 
      directive.action match {
        case Launch => {
          sender ! LaunchTask(directive.task)
          pendingQueue = pendingQueue.enqueue(directive)
          directiveQueue = rest 
        }
        case Kill => // Kill will be issued when receiveDirective, not here.
        case Resume => {
          sender ! ResumeTask(directive.task)
          pendingQueue = pendingQueue.enqueue(directive)
          directiveQueue = rest  
        }
        case _ => {
          LOG.warning("Unknown action {} for task {} from master {}", 
                              directive.action, directive.task.getId, 
                              directive.master)
          directiveQueue = rest
        }
      }
    }
  }

  override def receive = launchAck orElse resumeAck orElse killAck orElse pullForExecution orElse taskRequest orElse receiveDirective orElse isServiceReady orElse mediatorIsUp orElse isProxyReady orElse timeout orElse superviseeIsTerminated orElse unknown


}
