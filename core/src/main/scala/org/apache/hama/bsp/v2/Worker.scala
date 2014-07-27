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
import java.net.URL
import java.net.URLClassLoader
import org.apache.hama.Agent
import org.apache.hama.HamaConfiguration

final case class Bind(conf: HamaConfiguration, actorSystem: ActorSystem)
final case class ConfigureFor(task: Task)
final case class Execute(conf: HamaConfiguration, taskConf: HamaConfiguration)

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
  def configureFor: Receive = {
    case ConfigureFor(task) => {
      peer match {
        case Some(found) => {
          found.configureFor(task)
        }
        case None => LOG.warning("Unable to configure for task "+task+
                                 " because BSPPeer is missing!")
      }
    }
  }

  /**
   * Start executing {@link Superstep}s accordingly.
   * @return Receive id partial function.
   */
  def execute: Receive = {
    case Execute(conf, taskConf) => doExecute(conf, taskConf)
  }

  /**
   * Execute supersteps according to the task configuration provided.
   * @param taskConf is HamaConfiguration specific to a pariticular task.
   */
  protected def doExecute(conf: HamaConfiguration, 
                          taskConf: HamaConfiguration) = peer match {
    case Some(found) => {
      addJarToClasspath(taskConf)
      val superstepBSP = BSP.get(conf, taskConf)
      superstepBSP.setup(found)
      superstepBSP.bsp(found)
    }
    case None => LOG.error("BSPPeer is missing!")
  }

  /**
   * Dynamically add client jar url to the task configuration.
   * @param taskConf is the configuration sepcific to a task.
   * @return Option[ClassLoader] contains class loader with client jar url.
   */
  def addJarToClasspath(taskConf: HamaConfiguration): Option[ClassLoader] = {
    val jar = taskConf.get("bsp.jar")
    LOG.info("Jar path found in task configuration is {}", jar)
    jar match {
      case null => None
      case "" => None
      case url@_ => {
        val loader = Thread.currentThread.getContextClassLoader
        val newLoader = new URLClassLoader(Array[URL](new URL(url)), loader) 
        taskConf.setClassLoader(newLoader) 
        Some(newLoader)
      }
    }
  }

  override def receive = bind orElse configureFor orElse execute orElse unknown
}
