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
import org.apache.hama.bsp.v2.GroomServerStat
import org.apache.hama.bsp.v2.Task
import org.apache.hama.HamaConfiguration
import org.apache.hama.LocalService
import org.apache.hama.RemoteService
import org.apache.hama.ProxyInfo
import org.apache.hama.master._
import org.apache.hama.master.Directive._
import org.apache.hama.master.Directive.Action._

import scala.collection.immutable.Queue
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

final case object ContainerIsActive

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
   * Active scheduled tasks are stored in queue. 
   */
  private var queue = Queue[Directive]()

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
      slots ++= Set(Slot(seq, None, bspmaster))
    }
    LOG.debug("{} GroomServer slots are initialied.", constraint)
  }

  override def initializeServices {
    initializeSlots(getMaxTasks)
    lookup("sched", schedPath)
    lookup("groomManager", groomManagerPath)
  }

  def hasTaskInQueue: Boolean = !queue.isEmpty

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
        } else {
          LOG.debug("{} tasks in queue, process them first!", queue.size)
          val (directive, rest) = queue.dequeue
          if(null == directive)
            throw new NullPointerException("Dequeued directive is null!")
          matchThenExecute(directive.action, directive.master, 
                           directive.timestamp, directive.task)
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
    queue.foreach( directive => {
      if(null != directive) 
        stat.addToQueue(directive.task.getId.toString)
      else {
        LOG.warning("Directive shouldn't be null!")
        stat.addToQueue("(null)")
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

  /**
   * Receive {@link Directive} from Scheduler.
   * It may store directive in queue when no free slot available. 
   * @return Receive is partial function.
   */
  def receiveDirective: Receive = {
    case directive: Directive => { 
      if(null != directive) {
        if(hasFreeSlots) {
          if(hasTaskInQueue) {
            queue = queue.enqueue(directive)
            val (first, rest) = queue.dequeue

            LOG.info("Execute task {} in queue, which is sent from {} "+
                     "with action {} at {}", first.task.getId, first.master, 
                     first.action, first.timestamp) 
            matchThenExecute(first.action, first.master, first.timestamp, 
                             first.task)
          } else {
            LOG.info("Execute task {} sent from {} with action {} at {}", 
                     directive.task.getId , directive.master, 
                     directive.action, directive.timestamp) 
            matchThenExecute(directive.action, directive.master, 
                             directive.timestamp, directive.task)
          }
        } else {
          val task: Task = directive.task  
          if(null != task) 
            LOG.info("Enqueue directive: action-> {}, master-> {} , task-> {}", 
                     directive.action, directive.master, task.getId.toString)
          queue = queue.enqueue(directive)
        }
      } else LOG.warning("Directive dispatched from {} is null!", 
                         sender.path.name)
    }
  }

  /**
   * A notification when a container/ process is ready.
   */
  def containerIsActive: Receive = {
    case ContainerIsActive => {
      LOG.info("{} is ready for processing a task!", sender.path.name)
      val forked = sender
      children ++= Map(sender.path.name -> forked)// cache actorRef
      // TODO: start assigning a task
    }
  }

  override def receive = taskRequest orElse receiveDirective orElse isServiceReady orElse mediatorIsUp orElse isProxyReady orElse timeout orElse superviseeIsTerminated orElse unknown

  /**
   * Pick up a slot that is not in use.
   * @return Option[Slot] contains either a slot or None if no slot found.
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
   * Match an action from the {@link Directive} supplied, then execute
   * the task by forking a process if needed.
   */
  private def matchThenExecute(action: Action, master: String, 
                               timestamp: Long, task: Task) {
    action.value match {
      case 1 => {
        // a. pick up a free slot. 
        val slot = pickUp 
        //if(!slot.isRunning) {
          // TODO: create executor actor with different names 
          //       e.g. task attempt id so that actor won't collide and the 
          //       instance var e.g. process won't be the overriden.
          //context.actorOf(Props(classOf[Executor], conf), task.getId.toString)
        //}
        // b. check if process is forked -> check seq if booked.
        // b1. if false, fork a new process else reuse it.
        // c. launch task on the new process
        // d. store the task to a slot
        // e. periodically update stat to plugin
      }
      case 2 => 
      case 3 =>
      case 4 =>
      case _ => LOG.warning("Unknown action value "+action)
    }
  }
}
