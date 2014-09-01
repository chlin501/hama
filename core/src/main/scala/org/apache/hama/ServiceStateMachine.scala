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

import akka.actor.ActorRef
import akka.actor.FSM
import akka.actor.Cancellable
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration.DurationInt

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

  /**
  protected def afterLoaded(service: ActorRef) { } // TODO: remove this!
   */

  startWith(StartUp, Cache(Set.empty[ActorRef]))

  /**
   * Handle events in StartUp state.
   */
  when(StartUp) {
    case Event(Next, s @ Cache(subServices)) => {
      goto(Normal) using s
    }
  }

  /**
   * Handle events in Normal state.
   */
  when(Normal) {
    case Event(Next, s @ Cache(subServices)) => {
      //broadcast(Normal)
      //notify(Normal)(Ready(name)) 
      goto(CleanUp) using s
    }
  }

  /**
   * Handle events in CleanUp state.
   */
  when(CleanUp) {
    case Event(Next, s @ Cache(subServices)) => {
      goto(Stopped) using s
    }
  }

  /**
   * Capture unhandled events.
   * StateChecker type is a special event that used to check if current state 
   * is matched. 
   * For instance, if current state, i.e. stateName, is Normal, then notify 
   * listeners with Ready message so that clients can react after services are 
   * ready.
   */
  whenUnhandled {
    case Event(WhichState, s @ Cache(subServices)) => stateName match {
      case Normal => {
        if(!isNotifiedInNormal) {
          broadcast(Normal)
          notify(Normal)(Ready(name)) 
          isNotifiedInNormal = true
        }
        stay using s // FSM State
      }
      case Stopped => {
        if(!isNotifiedInStopped) { 
          stateChecker.cancel
          notify(Stopped)(Halt(name))
          isNotifiedInStopped = true
        }
        stay using s // FSM State
      }
      case state@_ => stay using s // FSM State
    }
    case Event(e, s) => {
      LOG.warning("CurrentState {}, unknown event {}, services {}.", 
                  stateName, e, s)
      stay using s
    }
  }

  /**
   * Broadcast to sub services.
   * @param state denotes which state the service is in now.
   */
  protected def broadcast(state: ServiceState) {
    state match {
      case StartUp => 
      case Normal => services.foreach(service => service ! MediatorIsUp)
      case CleanUp =>
      case Stopped =>
      case s@_ => LOG.warning("Can't broadcast because unknown state {}", s)
    }
  }

  protected def serviceStateListenerManagement: Receive = {
    case SubscribeState(state, ref) => stateListeners.get(state) match {
      case Some(foundRefs) => stateListeners = stateListeners.mapValues { 
          refs => refs + ref 
      }
      case None => stateListeners ++= Map(state -> Set(ref))
    }
    case UnsubscribeState(state, ref) => stateListeners.get(state) match {
      case Some(foundRefs) => stateListeners = stateListeners.mapValues { 
        refs => refs - ref 
      }
      case None => 
        LOG.warning("Actor not matched to unsubscribe with state {}.", state) 
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
