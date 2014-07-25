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
package org.apache.hama.bsp.v2

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import org.apache.hama.Agent
import org.apache.hama.HamaConfiguration

final case class Bind(conf: HamaConfiguration, actorSystem: ActorSystem)
final case class Initialize(task: Task)
final case class Execute(conf: HamaConfiguration)

class Worker extends Agent {

  protected var peer: Option[Coordinator] = None

  protected def bind(old: Option[Coordinator], 
                     conf: HamaConfiguration, 
                     actorSystem: ActorSystem): Option[Coordinator] = 
  old match {
    case None => Some(Coordinator(conf, actorSystem))
    case Some(peer) => old
  } 

  def bind: Receive = {
    case Bind(conf, actorSystem) => {
      this.peer = bind(this.peer, conf, actorSystem) 
    }
  }

  /**
   * This ties coordinator to a particular task.
   */
  def initialize: Receive = {
    case Initialize(task) => {
      peer match {
        case Some(found) => {
          found.configureFor(task)
        }
        case None => LOG.warning("Unable to initialize task "+task)
      }
    }
  }

  /**
   * Start executing {@link Superstep}s accordingly.
   * @return Receive id partial function.
   */
  def execute: Receive = {
    case Execute(conf) => doExecute(conf)
  }

  protected def doExecute(conf: HamaConfiguration) {
    peer match {
      case Some(found) => {
        val superstepBSP = BSP.get(conf)
        superstepBSP.setup(found)
        superstepBSP.bsp(found)
      }
      case None => LOG.warning("BSPPeer is missing!")
    }
  }

  override def receive = bind orElse initialize orElse execute orElse unknown
}
