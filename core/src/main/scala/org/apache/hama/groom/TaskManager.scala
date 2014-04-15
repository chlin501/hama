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

import akka.actor._
import org.apache.hama.bsp.v2._
import org.apache.hama._
import org.apache.hama.master._
import org.apache.hama.master.Directive._
import org.apache.hama.master.Directive.Action._

import scala.collection.immutable.Queue
import scala.concurrent.duration._

final case object ChildIsActive

class TaskManager(conf: HamaConfiguration) extends LocalService 
                                           with RemoteService {

  val schedInfo =
    new ProxyInfo.Builder().withConfiguration(conf).
                            withActorName("sched").
                            appendRootPath("bspmaster").
                            appendChildPath("sched").
                            buildProxyAtMaster

  val schedPath = schedInfo.getPath

  val groomServerHost = conf.get("bsp.groom.address", "127.0.0.1")
  val groomServerPort = conf.getInt("bsp.groom.port", 50000)
  val groomServerName = "groom_"+ groomServerHost +"_"+ groomServerPort

  var sched: ActorRef = _
  var cancellable: Cancellable = _

  val maxTasks = configuration.getInt("bsp.tasks.maximum", 3) 
  val bspmaster = configuration.get("bsp.master.name", "bspmaster")

  /**
   * The max size of slots can't exceed configured maxTasks.
   */
  private var slots = Set.empty[Slot]

  private var queue = Queue[Task]()

  override def configuration: HamaConfiguration = conf

  override def name: String = "taskManager"

  protected def initializeSlots {
    for(seq <- 1 to maxTasks) {
      slots ++= Set(Slot(seq, None, bspmaster, None))
    }
    LOG.debug("{} GroomServer slots are initialied.", maxTasks)
  }

  override def initializeServices {
    initializeSlots     
    lookup("sched", schedPath)
  }

  def hasTaskInQueue: Boolean = queue.size > 0

  def hasFreeSlots(): Boolean = {
    var isFree = true
    var flag = true
    slots.takeWhile(slot => {
      isFree = None.equals(slot.task)
      isFree
    }) 
    if(isFree) flag = true else flag = false
    flag
  }

  /**
   * Periodically send message to an actor an actor.
   */
  def request(target: ActorRef, message: RequestMessage) {
    LOG.debug("Request message {} to target: {}", message, target)
    import context.dispatcher
    cancellable = 
      context.system.scheduler.schedule(0.seconds, 5.seconds, target,
                                        message)
  }

  override def afterLinked(proxy: ActorRef) = {
    sched = proxy  
    request(self, TaskRequest)
  }

  /**
   * Check if slots available and any unprocessed task in queue.
   * If slots are free, request scheduler to dispatch tasks accordingly.
   * Otherwise deal with task in queue first.
   */
  def requestMessage: Receive = {
    case TaskRequest => {
      LOG.debug("In TaskRequest, sched: {}, hasTaskInQueue: {}"+
               ", hasFreeSlots: {}", sched, hasTaskInQueue, hasFreeSlots)
      if(!hasTaskInQueue && hasFreeSlots) { 
        // TODO: also check sys load,  memory, etc.
        LOG.debug("Request {} for assigning new tasks ...", schedPath)
        request(sched, RequestTask(groomServerName)) 
      } else {
        // TODO: process task in queue first.
      }
    }
  }

  /**
   * Receive Directive from Scheduler.
   */
  def receiveDirective: Receive = {
    case directive: Directive => { 
       val action: Action = directive.action  
       val master: String = directive.master  
       val timestamp: Long = directive.timestamp  
       val task: Task = directive.task  
       LOG.info("Action {} sent from {} for {} at {}", 
                action, master, task, timestamp) 
       matchThenExecute(action, master, timestamp, task)
    }
  }

  def childIsActive: Receive = {
    case ChildIsActive => {
      // start assign a task
    }
  }

  override def receive = requestMessage orElse receiveDirective orElse isServiceReady orElse serverIsUp orElse isProxyReady orElse timeout orElse unknown

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

  private def matchThenExecute(action: Action, master: String, 
                               timestamp: Long, task: Task) {
    action.value match {
      case 1 => {
        // a. pick up a free slot. 
        val slot = pickUp
        //configuration.setInt("bsp.task.child.seq", )
        // b. fork a new process 
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

final case object RequestAssign

class BSPChild(conf: HamaConfiguration) extends LocalService 
                                                    with RemoteService {

  /* Task assigned from TaskManager for computation. */
  var task: Task = _

  /* GroomServer TaskManager proxy. */
  var taskManager: ActorRef = _
  
  /* TaskManager cancellable object. */
  var cancelTaskManager: Cancellable = _

  /* Sequence denote which slots is occipued. */
  var seq: Int = configuration.getInt("bsp.task.child.seq", -1)

  val taskManagerInfo = 
    new ProxyInfo.Builder().withConfiguration(configuration).
                            withActorName("taskManager").
                            appendRootPath("bspmaster").
                            appendChildPath("taskManager").
                            buildProxyAtMaster

  val taskManagerPath = taskManagerInfo.getPath

  override def configuration: HamaConfiguration = conf
  override def name = "bspChild"
 
  override def initializeServices {
    lookup("taskManager", taskManagerPath) 
  }

  override def afterLinked(proxy: ActorRef) = {
    taskManager = proxy
    cancelTaskManager = request(taskManager, RequestAssign)
  }

  def request(target: ActorRef, message: Any): Cancellable = {
    import context.dispatcher
    context.system.scheduler.schedule(0.seconds, 2.seconds, target, message)
  }

  override def receive = isProxyReady orElse timeout orElse unknown
}
