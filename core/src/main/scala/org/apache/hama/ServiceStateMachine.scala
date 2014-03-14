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

sealed trait StateMessage
case class SubscribeState(state: ServiceState, ref: ActorRef) 
     extends StateMessage
case class UnsubscribeState(state: ServiceState, ref: ActorRef) 
     extends StateMessage

sealed trait HamaServices
private case class Cache(services: Set[ActorRef]) extends HamaServices

sealed trait StateChecker
private case object WhichState extends StateChecker

/**
 * This trait defines generic states a system will use.
 */
trait ServiceStateMachine extends FSM[ServiceState, HamaServices] 
                          with LocalService {

  /**
   * Mapping ServiceState to a set of Actor to be notified.
   */
  private var stateListeners = Map.empty[ServiceState, Set[ActorRef]]
  var stateChecker: Cancellable = _
  var isNotifiedInNormal = false 
  var isNotifiedInStopped = false

  /**
   * Periodically check if <ServiceState>.equals(stateName).
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
    stateChecker = check(WhichState)
  }

  startWith(StartUp, Cache(Set.empty[ActorRef]))

  /**
   * Handle events in StartUp state.
   */
  when(StartUp) {
    case Event(Load, s @ Cache(subServices)) => {
      LOG.info("Loading service {}", sender.path.name)  
      val currentServices = subServices ++ Set(sender)
      services = currentServices // Service.scala#services
      LOG.debug("Current services available: {}", services.mkString(", "))
      context.watch(sender) // watch sub service
      cancelServiceLookup(sender.path.name, sender)
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
    case Event(Shutdown, s @ Cache(subServices)) => {
      LOG.info("Shutting down server ...")
      subServices.view.foreach {
        case service => {
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
    case Event(Unload, s @ Cache(subServices)) => {
      val currentServices = subServices - sender 
      context.unwatch(sender) // unwatch sub service
      val cache = s.copy(currentServices)
      if(0 == currentServices) {
        goto(Stopped) using cache
      } else {
        stay using cache
      }
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
    case Event(WhichState, s @ Cache(subServices)) => {
      var tmp: State = stay using s // FSM State
      if(Normal.equals(stateName) && !isNotifiedInNormal) {
        LOG.debug("StateName [{}] should be Normal.", stateName)
        notify(Normal)(Ready(name))
        isNotifiedInNormal = true
      } else if(Stopped.equals(stateName) && !isNotifiedInStopped) {
        LOG.debug("StateName [{}] should be Stopped.", stateName)
        // TODO: check if subServices map is empty, if not throws exception
        stateChecker.cancel
        notify(Stopped)(Halt(name))
        isNotifiedInStopped = true
        tmp = stop(FSM.Normal, Cache(Set.empty[ActorRef]))
      }
      tmp
    } 
    /**
     * Event such as Load, SubscribeState may go to here
     */
    case Event(e, s) => {
      LOG.warning("CurrentState {}, unknown event {}, services {}.", 
                  stateName, e, s)
      stay using s
    }
  }

  protected def serviceStateListenerManagement: Receive = {
    case SubscribeState(state, ref) => {
      stateListeners.get(state) match {
        case Some(refs) => {
          stateListeners = stateListeners.mapValues { 
            refs => refs + ref 
          }
        }
        case None => {
          stateListeners ++= Map(state -> Set(ref))
        }
      }
      LOG.debug("Mapping: {}", stateListeners.mkString(", "))
    }
    case UnsubscribeState(state, ref) => {
      stateListeners.get(state) match {
        case Some(refs) => {
          stateListeners = stateListeners.mapValues { 
            refs => refs - ref 
          }
        }
        case None => 
          LOG.warning("No matching actor to unsubscribe with state {}.", state) 
      }
    }
  }

  /**
   * Notify listeners when a specific state is triggered.
   */
  protected def notify(state: ServiceState)(message: Any): Unit = {
    LOG.debug("state: {}, message, {}, stateListeners {}", 
              state, message, stateListeners.mkString(", "))
    stateListeners.filter(p => state.equals(p._1)).values.flatten.
                   foreach( ref => {
      LOG.debug("Send msg to ref {}!", ref)
      ref ! message
    })
  }

  initialize()
}
