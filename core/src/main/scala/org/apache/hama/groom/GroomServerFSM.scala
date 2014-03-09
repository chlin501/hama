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
import akka.event._
import org.apache.hama._
import scala.concurrent.duration._

sealed trait HamaServices
case object Uninitialized extends HamaServices
case class Cache(services: Map[String, ActorRef]) extends HamaServices

private[groom] case class WhichState(stateName: State)
// hack for akka fsm don't know its current state by itself.
// https://groups.google.com/d/msg/akka-user/0UVhOP_agIA/vyVQdbizUVYJ
private[groom] final class CurrentStateChecker extends Actor {
  val LOG = Logging(context.system, this)
  def receive = {
    case WhichState(stateName) => {
      if(Normal.equals(stateName)) {
        LOG.info("Current state {} is Normal. Trigger InNormal event.", 
                 stateName)
        sender ! InNormal
      }
    }
  }
}

trait GroomServerFSM extends FSM[State, HamaServices] with Service 
                                                      with GroomStateListener {

  var checkCancellable: Cancellable = _

  override def preStart { 
    val checker = 
      context.system.actorOf(Props(classOf[CurrentStateChecker]))
    import context.dispatcher
    checkCancellable = 
      context.system.scheduler.schedule(0.seconds, 3.seconds, checker, 
                                        WhichState(stateName))
  }

  startWith(StartUp, Uninitialized)

  /**
   * Handle events in StartUp state.
   */
  when(StartUp) {
    case Event(Init, Uninitialized) => {
      LOG.info("Initialize services ...")
      stay using Cache(Map.empty[String, ActorRef])
    }

    case Event(Load(name, service), s @ Cache(prevServices)) => {
      LOG.info("Loading service {}", name)
      val currentServices = prevServices ++ Map(name -> service)
      services = currentServices // services in Services.scala
      context.watch(service)
      cancelServicesWhenReady.get(name) match {
        case Some(cancellable) => cancellable.cancel
        case None =>
          LOG.warning("Can't cancel for service {} not found!", name)
      }
      val cache = s.copy(currentServices)
      if(servicesCount == currentServices.size) {
        goto(Normal) using cache
      } else {
        stay using cache
      }
    }
  }

  /**
   * Handle events in Normal state.
   */
  when(Normal) {
    case Event(InNormal, s @ Cache(services)) => {
      checkCancellable.cancel
      LOG.info("Notify all listeners that Groom is in Normal state.") 
      notifyAllWith(Normal)(GroomIsReady)
      stay using s
    }
    case Event(Shutdown, s @ Cache(services)) => {
      LOG.info("Shutting down server ...")
      services.view.foreach{
        case (name, service) => {
          // service once receive Shutdown message MUST perform housekeeping 
          // (cleanup) tasks. In the end call sender ! Unload(name) where name
          // is the def of its own function name.
          // and all services MUST stop accepts requests. 
          service ! Shutdown 
        }
      }
      goto(CleanUp) using s
    }
  }

  /**
   * Handle events in CleanUp state.
   */
  when(CleanUp) {
    case Event(Unload(name), s @ Cache(services)) => {
      val currentServices = services - name 
      val cache = s.copy(currentServices)
      if(0 == currentServices) {
        goto(Stopped) using cache
      } else {
        stay using cache
      }
    }
  }

  /**
   * Capture unhandled event 
   */
  whenUnhandled {
    case Event(e, s) => {
      LOG.warning("Unknown event {} with services {}.", e, s)
      stay
    }
  }

  // TODO: need to model fail state?

  /** 
   * onTransition is needed only when the server needs to perform steps before
   * State is transferred to the next one.
   * It's `stateData' will be the old one before replaced with the new data.
   * For example, in CleanUp -> Stopped, stateData will have 1 services left, 
   * instead of 0.
  onTransition {
    case StartUp -> Normal => { }
    case Normal -> CleanUp => { }
    case CleanUp -> Stopped => { }
  }
   */

  initialize

}

