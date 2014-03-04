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

import akka.actor._
import akka.actor.SupervisorStrategy._
import akka.event._
import scala.concurrent.duration._
import org.apache.hama.HamaConfiguration
import org.apache.hama.sched.Scheduler
import org.apache.hama.monitor.DefaultMonitor

class Master(conf: HamaConfiguration) extends Actor {

  val LOG = Logging(context.system, this)

  var services = Map.empty[String, ActorRef]
  var cancelWhenReady = Map.empty[String, Cancellable]
 
  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 3, withinTimeRange = 1 minute) {
      case _: NullPointerException     => Restart
      case _: IllegalArgumentException => Stop
      case _: Exception                => Escalate
    }
  
  def create[A <: Actor](name: String, target: Class[A]) {
    val actor = context.actorOf(Props(target, conf), name)
    import context.dispatcher
    val cancellable = 
      context.system.scheduler.schedule(0.seconds, 2.seconds, actor, Ready)
    cancelWhenReady ++= Map(name -> cancellable)
  }

  def initialize() {
    create("groomManager", classOf[GroomManager]) 
    create("monitor", classOf[DefaultMonitor]) 
    create("sched", classOf[Scheduler]) 
    create("receptionist", classOf[Receptionist]) 
/*
    val monitor = 
      context.actorOf(Props(classOf[DefaultMonitor], conf), "monitor")
    monitor ! Ready
    val sched = context.actorOf(Props(classOf[Scheduler], conf), "sched")
    sched ! Ready
    val receptionist = 
      context.actorOf(Props(classOf[Receptionist], conf), "receptionist")
    receptionist ! Ready
*/
  }

  override def preStart() {
    initialize() 
  }

  override def receive = {
    case Ready => {
      if(4 != services.size) {
        LOG.info("Currently {} services are ready.", services.size)
        sender ! Ack("no")
      } else {
        sender ! Ack("yes")
      }     
    }
    case Ack(name) => {
      LOG.info("Actor {} is ready.", name)
      services ++= Map(name -> sender)
      context.watch(sender) 
      cancelWhenReady.get(name) match {
        case Some(cancellable) => cancellable.cancel
        case None => LOG.warning("Can't cancel message for {}.", name)
      }
    }
    case _ => {
      LOG.warning("Unknown message for master.")
    }
  }
}
