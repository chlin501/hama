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

import org.apache.hama.bsp.v2._
import org.apache.hama._
import org.apache.hama.master._
import org.apache.hama.master.Directive._
import org.apache.hama.master.Directive.Action._

class TaskManager(conf: HamaConfiguration) extends LocalService {

  val maxTasks = conf.getInt("bsp.tasks.maximum", 3) 
  val bspmaster = configuration.get("bsp.master.name", "bspmaster")
  /**
   * The max size of slots can't exceed configured maxTasks.
   */
  private var slots = Set.empty[Slot]

  override def configuration: HamaConfiguration = conf

  override def name: String = "taskManager"

  private def initializeSlots {
    for(seq <- 1 to maxTasks) {
      slots ++= Set(Slot(seq, None, bspmaster))
    }
  }

  override def initializeServices {
    initializeSlots     
  }

  def receiveDirective: Receive = {
    case directive: Directive => { 
       val action: Action = directive.action  
       val master: String = directive.master  
       val timestamp: Long = directive.timestamp  
       LOG.info("{} action from {} at {}", action, master, timestamp) 
       matchThenExecute(action, master, timestamp)
    }
  }

  override def receive = receiveDirective orElse isServiceReady orElse serverIsUp orElse unknown

  private def matchThenExecute(action: Action, master: String, 
                               timestamp: Long) {
    action.value match {
      case 1 => {
        // create a new task
        //val task = createTask // TODO: check task is created at groom or master?
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

/*
  def createTask(): Task = {
    new Task.Builder().//setId().
                       //setStartTime().
                       build
  } 
*/
}
