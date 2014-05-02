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

import akka.actor.ActorRef
import akka.actor.Cancellable
import org.apache.hama.HamaConfiguration
import org.apache.hama.LocalService
import org.apache.hama.Request
import org.apache.hama.bsp.v2.GroomServerStat
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration
import scala.collection.immutable.Queue

final case class GroomRegistration(taskManager: ActorRef, 
                                   groomServerName: String,
                                   var notifySched: Boolean = false) 
private[master] final case object NotifyScheduler

/**
 * A service that manages a set of {@link org.apache.hama.groom.GroomServer}s.
 * It basically performs two tasks: 
 * - Receive GroomServer's TaskManager registration.
 * - Notify Scheduler by sending TaskManager actor reference.
 * @param conf contains specific configuration for this service. 
 */
class GroomManager(conf: HamaConfiguration) extends LocalService {

  type GroomHostName = String
  type CrashCount = Int

  private var registrationWatcher: Cancellable = _

  /* Store the GroomServerStat information.*/
  private[this] var grooms = Set.empty[GroomRegistration] 

  /**
   * Identical GroomServer host name logically represents the same GroomServer, 
   * even if the underlying hardware is changed.
   * TODO: may need to reset crash count or 
   *       store more offline line grooms stat info.
   */
  private[this] var offlineGroomsStat = Map.empty[GroomHostName, CrashCount] 

  override def configuration: HamaConfiguration = conf

  override def name: String = "groomManager"

  /**
   * Quarantine offline GroomServer.
   */
  def quarantine(offline: ActorRef, resched:(GroomHostName) => Unit) {
    grooms.find(p=>p.taskManager.equals(offline)) match {//move to offlineGrooms
      case Some(groom) => {
        grooms -= groom 
        offlineGroomsStat.get(groom.groomServerName) match { // update
          case Some(crashCount) => 
            offlineGroomsStat = offlineGroomsStat.mapValues{ cnt => cnt + 1 }
          case None => offlineGroomsStat ++= Map(groom.groomServerName -> 1)
        }
        resched(groom.groomServerName) 
      }
      case None => 
        LOG.warning("GroomServer {} is watched but not found in the list!")
    }
    LOG.info("OfflineGroomsStat: {}", offlineGroomsStat.mkString(", "))
  }

  /**
   * Call {@link Scheduler} to reschedule tasks in failure {@link GroomServer}.
   * @param stat contains all tasks in failure GroomServer.
   */
  def rescheduleTasks(groomServerName: GroomHostName) { 
    if(null != mediator) 
      mediator ! Request("sched", RescheduleTasks(groomServerName))
    else 
      LOG.warning("Mediator is not ready so rescheduling is impossible!")
  }

  override def offline(taskManager: ActorRef) {
    quarantine(taskManager, rescheduleTasks)
  }

  def checkIfRejoin(from: ActorRef, groomServerName: String) {
    grooms ++= Set(GroomRegistration(from, groomServerName))
    // TODO: 
    // 1. specific stat info recording groom crash info.
    // 2. if stat is with refresh hardware, reset crashed count to 0
  }
  
  override def afterMediatorUp {
    import context.dispatcher
    registrationWatcher = 
      context.system.scheduler.schedule(0.seconds, 5.seconds, self, 
                                        NotifyScheduler)
  }

  /**
   * GroomServer's TaskManager register itself for monitored.
   * N.B.: Mediator may not be up at this momeent.
   */
  def register: Receive = {
    //case stat: GroomServerStat => { 
    case Register(groomServerName) => {
      LOG.info("{} requests to register {}.", sender.path.name) 
      checkIfRejoin(sender, groomServerName)
      context.watch(sender) // watch remote taskManager
    }

  }

  def registration: Receive = {
    case NotifyScheduler => {
      grooms.foreach( groom => {
        if(!groom.notifySched) {
          if(null == mediator)
            throw new RuntimeException("Mediator is missing!")
          mediator ! Request("sched", 
                             GroomEnrollment(groom.groomServerName, 
                                             groom.taskManager))
          groom.notifySched = true
        }
      })
    } 
  }

  override def receive = register orElse isServiceReady orElse serverIsUp orElse superviseeIsTerminated orElse unknown

}
