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

final case class Bind(conf: HamaConfiguration, actorSystem: ActorSystem,
                      container: ActorRef)
final case class Initialize(task: Task)
final case object Execute

class Worker extends Agent {

  protected var coordinator: Coordinator = _
  protected var container: ActorRef = _

  protected def bind1(old: Coordinator, 
                      conf: HamaConfiguration, 
                      actorSystem: ActorSystem): Coordinator = old match {
    case null => Coordinator(conf, actorSystem)
    case _ => old
  } 

  protected def bind2(ref: ActorRef, param: ActorRef): ActorRef = ref match {
    case null => param
    case _ => ref
  }

  def bind: Receive = {
    case Bind(conf, actorSystem, container) => {
      this.coordinator = bind1(this.coordinator, conf, actorSystem) 
      this.container = bind2(this.container, container)
    }
  }

  /**
   * This ties coordinator to a particular task.
   */
  def initialize: Receive = {
    case Initialize(task: Task) => {
      coordinator.configureFor(task)
    }
  }

  def execute: Receive = {
    case Execute => {

    }
  }

  override def receive = bind orElse initialize orElse unknown
}

