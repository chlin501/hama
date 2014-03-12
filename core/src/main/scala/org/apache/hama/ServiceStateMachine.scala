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
private case class Cache(services: Map[String, ActorRef]) extends HamaServices

sealed trait StateChecker
private case object WhichState extends StateChecker

/**
 * This trait defines generic states a system will use.
 */
trait ServiceStateMachine extends FSM[ServiceState, HamaServices] with Service {

  /**
   * Mapping ServiceState to a set of Actor to be notified.
   */
  var mapping = Map.empty[ServiceState, Set[ActorRef]]
  var stateChecker: Cancellable = _
  var stoppedStateChecker: Cancellable = _

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

  startWith(StartUp, Cache(Map.empty[String, ActorRef]))

  /**
   * Handle events in StartUp state.
   */
  when(StartUp) {
    case Event(Load(name, service), s @ Cache(prevServices)) => {
      LOG.info("Loading service {}", name)
      val currentServices = prevServices ++ Map(name -> service)
      services = currentServices // Service.scala#services
      LOG.debug("Current services available: {}", services.mkString(", "))
      context.watch(service)
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

  /**
   * Capture unhandled events.
   * StateChecker type is a special event that used to check if current state is 
   * matched. 
   * For instance, if current state, i.e. stateName, is Normal, then notify 
   * listeners with Ready message so that clients can react after services are 
   * ready.
   */
  whenUnhandled {
    case Event(WhichState, s @ Cache(services)) => {
      var tmp: State = stay using s // FSM State
      if(Normal.equals(stateName)) {
        LOG.debug("StateName [{}] should be Normal.", stateName)
        stateChecker.cancel
        notify(Normal)(Ready(name))
      } else if(Stopped.equals(stateName)) {
        LOG.debug("StateName [{}] should be Stopped.", stateName)
        // TODO: check if services map is empty, if not throws exception
        stoppedStateChecker.cancel
        notify(Stopped)(Halt(name))
        tmp = stop(FSM.Normal, Cache(Map.empty[String, ActorRef]))
      }
      tmp
    } 
    /**
     * Event such as Load, SubscribeState may go to here
     */
    case Event(e, s) => {
      LOG.warning("[current state:{}] Unknown event {} with services {}.", 
                  stateName, e, s)
      stay using s
    }
  }

  protected def serviceStateListenerManagement: Receive = {
    case SubscribeState(state, ref) => {
      mapping.get(state) match {
        case Some(refs) => {
          mapping = mapping.mapValues { refs => refs+ref}
        }
        case None => {
          mapping ++= Map(state -> Set(ref))
        }
      }
      LOG.debug("Mapping: {}", mapping)
    }
    case UnsubscribeState(state, ref) => {
      mapping.get(state) match {
        case Some(refs) => {
          mapping = mapping.mapValues { refs => refs-ref}
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
    LOG.debug("state: {}, message, {}, mapping {}", state, message, mapping)
    mapping.filter(p => state.equals(p._1)).values.flatten.foreach( ref => {
      LOG.debug("Send msg to ref {}!", ref)
      ref ! message
    })
  }

  initialize()

}
