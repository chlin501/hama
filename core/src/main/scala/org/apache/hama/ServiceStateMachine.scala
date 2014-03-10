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
package org.apache.hama

import akka.actor._
import akka.event._
import scala.concurrent.duration._

sealed trait HamaServices
private case object Uninitialized extends HamaServices
private case class Cache(services: Map[String, ActorRef]) extends HamaServices

sealed trait StateChecker
private case object IsNormal extends StateChecker
private case object IsStopped extends StateChecker

/**
 * This trait defines generic states a system will use.
 */
trait ServiceStateMachine extends FSM[State, HamaServices] 
                          with Service with ServiceStateListener {

  var normalStateChecker: Cancellable = _
  var stoppedStateChecker: Cancellable = _

  /**
   * Periodically check if <State>.equals(stateName).
   * If ture, act based upon that state e.g. notify clients.
   */
  private def check(stateChecker: StateChecker,
                    delay: FiniteDuration = 3.seconds): Cancellable = {
    import context.dispatcher
    context.system.scheduler.schedule(0.seconds, delay, self, 
                                      stateChecker)
  }

  override def preStart { 
    super.preStart
    normalStateChecker = check(IsNormal)
    stoppedStateChecker = check(IsStopped)
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
      cancelServicesChecker(name, service)
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
    case Event(IsNormal, s @ Cache(services)) => {
      LOG.info("StateName [{}] should be in Normal.", stateName)
      normalStateChecker.cancel
      notify(Normal)(Ready(name))
      stay using s 
    }
    case Event(Shutdown, s @ Cache(services)) => {
      LOG.info("Shutting down server ...")
      services.view.foreach {
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

  when(Stopped) {
    case Event(IsStopped, s @ Cache(services)) => {
      LOG.info("StateName [{}] should be in Stopped.", stateName)
      // TODO: check if services map is empty, if not throws exception
      stoppedStateChecker.cancel
      notify(Stopped)(Ready(name))
      stop(FSM.Normal, Uninitialized)
    }  
  }

  /**
   * Capture unhandled events.
   * StateChecker type is a special event that used to check if current state is 
   * matched. 
   * For instance, if current state, i.e. stateName, is Normal, then notify 
   * listeners with Ready message so that clients can react after services are 
   * ready.
   */
  whenUnhandled {
    case Event(e, s) => {
      LOG.warning("Unknown event {} with services {}.", e, s)
      stay using s
    }
  }

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
