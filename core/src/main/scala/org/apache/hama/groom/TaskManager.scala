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

class TaskManager(conf: HamaConfiguration) extends LocalService 
                                           with RemoteService {

  val schedInfo =
    ProxyInfo("sched",
              conf.get("bsp.master.actor-system.name", "MasterSystem"),
              conf.get("bsp.master.address", "127.0.0.1"),
              conf.getInt("bsp.master.port", 40000),
              "bspmaster/sched")

  val schedPath = schedInfo.path

  var sched: ActorRef = _
  var cancellable: Cancellable = _

  val maxTasks = conf.getInt("bsp.tasks.maximum", 3) 
  val bspmaster = configuration.get("bsp.master.name", "bspmaster")

  /**
   * The max size of slots can't exceed configured maxTasks.
   */
  private var slots = Set.empty[Slot]

  private var queue = Queue[Task]()

  override def configuration: HamaConfiguration = conf

  override def name: String = "taskManager"

  private def initializeSlots {
    for(seq <- 1 to maxTasks) {
      slots ++= Set(Slot(seq, None, bspmaster))
    }
    LOG.info("{} GroomServer slots are initialied.", maxTasks)
  }

  override def initializeServices {
    initializeSlots     
    lookup("sched", schedPath)
  }

  def hasTaskInQueue: Boolean = queue.size > 0

  def hasFreeSlots(): Boolean = {
    var occupied = true
    slots.takeWhile(slot => {
      occupied = None.equals(slot.task)
      occupied
    }) 
    !occupied
  }

  def request(target: ActorRef, message: RequestMessage) {
    import context.dispatcher
    cancellable = 
      context.system.scheduler.schedule(0.seconds, 5.seconds, target,
                                        message)
  }

  override def afterLinked(proxy: ActorRef) = {
    sched = proxy  
    request(self, TaskRequest)
  }

  def requestMessage: Receive = {
    case TaskRequest => {
      if(!hasTaskInQueue && hasFreeSlots) {
        LOG.info("Request {} for assigning new tasks ...", schedPath)
        request(sched, RequestTask) 
      }
    }
  }

  def receiveDirective: Receive = {
    case directive: Directive => { 
       val action: Action = directive.action  
       val master: String = directive.master  
       val timestamp: Long = directive.timestamp  
       val task: Task = directive.task  // move task to action
       LOG.info("{} action from {} at {}", action, master, timestamp) 
       matchThenExecute(action, master, timestamp, task)
    }
  }

  override def receive = requestMessage orElse receiveDirective orElse isServiceReady orElse serverIsUp orElse isProxyReady orElse timeout orElse unknown

  private def matchThenExecute(action: Action, master: String, 
                               timestamp: Long, task: Task) {
    action.value match {
      case 1 => {
        // arrange and store the task to a slot
        // fork a new process by actor
        // launch task on new process
        // send stat to plugin
      }
      case 2 => 
      case 3 =>
      case 4 =>
      case _ => LOG.warning("Unknown action value "+action)
    }
  }


}
