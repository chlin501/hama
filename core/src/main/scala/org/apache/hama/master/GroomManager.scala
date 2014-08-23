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

final case class GroomRegistration(groomServerName: String,
                                   taskManager: ActorRef, 
                                   maxTasks: Int,
                                   var notified: Boolean = false) 
private[master] final case object Notifying

/**
 * A service that manages a set of {@link org.apache.hama.groom.GroomServer}s.
 * It basically performs two tasks: 
 * - Receive GroomServer's TaskManager registration.
 * - Notify Scheduler by sending TaskManager actor reference.
 * @param conf contains specific configuration for this service. 
 */
class GroomManager(conf: HamaConfiguration) extends LocalService {

  type GroomServerName = String
  type CrashCount = Int

  private var registrationWatcher: Cancellable = _

  /** 
   * Store the GroomServerStat information.
   * We don't use Map because Set can "filter" for updating `notified' 
   * variable.
   */
  protected[this] var grooms = Set.empty[GroomRegistration] 
 
  /**
   * Identical GroomServer host name logically represents the same GroomServer, 
   * even if the underlying hardware is changed.
   * TODO: may need to reset crash count or 
   *       store more offline line grooms stat info.
   */
  private[this] var offlineGroomsStat = Map.empty[GroomServerName, CrashCount] 

  override def configuration: HamaConfiguration = conf

  /**
   * Quarantine offline GroomServer.
   */
  def quarantine(offline: ActorRef, reaction:(GroomServerName) => Unit) {
    grooms.find(p=>p.taskManager.equals(offline)) match {//move to offlineGrooms
      case Some(groom) => {
        grooms -= groom 
        offlineGroomsStat.get(groom.groomServerName) match { // update
          case Some(crashCount) => 
            offlineGroomsStat = offlineGroomsStat.mapValues{ cnt => cnt + 1 }
          case None => offlineGroomsStat ++= Map(groom.groomServerName -> 1)
        }
        reaction(groom.groomServerName) 
      }
      case None => 
        LOG.warning("GroomServer {} is watched but not found in the list!")
    }
    LOG.info("OfflineGroomsStat: {}", offlineGroomsStat.mkString(", "))
  }

  /**
   * Call {@link Scheduler} to reschedule tasks on {@link GroomServer} failure,
   * and update GroomServer stat to {@link Receptionist}.
   * @param stat contains all tasks in failure GroomServer.
   */
  def offlineReaction(groomServerName: GroomServerName) { 
    if(null != mediator) { 
      mediator ! Request("sched", RescheduleTasks(groomServerName))
      mediator ! Request("receptionist", GroomStat(groomServerName, 0))
    } else {
      // TODO: put Requests to queue and notify when mediator is up  
      LOG.warning("No mediator so offline reaction for {} is impossible!", 
                  groomServerName)
    }
  }

  override def offline(taskManager: ActorRef) {
    quarantine(taskManager, offlineReaction)
  }

  /**
   * Register GroomServer simple stat, including groom server name and maxTasks
   * to GroomManager. 
   * @param from which GroomServer the stat information is.
   * @param groomServerName denotes the name of the GroomServer. 
   * @param maxTasks tells the capacity, max tasks allowed, of the GroomServer.
   */
  def register(from: ActorRef, groomServerName: String, maxTasks: Int) {
    grooms.find(p=>p.groomServerName.equals(groomServerName)) match {
      case Some(found) => {   
        grooms -= found
        grooms ++= Set(GroomRegistration(groomServerName, from, maxTasks))
      }
      case None => 
        grooms ++= Set(GroomRegistration(groomServerName, from, maxTasks))
    }
    // TODO: 
    // 1. specific stat info recording groom crash info.
    // 2. if stat is with refresh hardware, reset crashed count to 0
  }
  
  override def afterMediatorUp {
    import context.dispatcher
    registrationWatcher =  // TODO: cancel watcher when shutting down. 
      context.system.scheduler.schedule(0.seconds, 3.seconds, self, Notifying)
  }

  /**
   * GroomServer's TaskManager enroll itself for being monitored.
   * N.B.: Mediator may not be up at this momeent.
   * @return Actor.Receive 
   */
  def enroll: Receive = {
    case reg: Register => {
      LOG.info("{} requests to renroll {}, which allows {} max tasks.", 
               sender.path.name, reg.getGroomServerName, reg.getMaxTasks) 
      register(sender, reg.getGroomServerName, reg.getMaxTasks)
      context.watch(sender) // watch remote taskManager
    }
  }

  /**
   * Periodically notify Scheduler GroomServers enroll.
   * Also update the flag notified if the taskManager actor is passed to 
   * {@link Scheduler}. {@link Receptionist} will also be notified with 
   * GroomServer's maxTasks.
   */
  def notifying: Receive = {
    case Notifying => {
      grooms.filter(groom => !groom.notified) match {
        case fresh: Set[GroomRegistration] => fresh.foreach( newjoin => {
          if(null == mediator)
            throw new RuntimeException("Impossible no mediator after it's up!")
          mediator ! Request("sched", GroomEnrollment(newjoin.groomServerName, 
                                                      newjoin.taskManager,
                                                      newjoin.maxTasks))
          
          mediator ! Request("receptionist", 
                             GroomStat(newjoin.groomServerName, 
                                       newjoin.maxTasks))
          newjoin.notified = true
        })
        case _ => 
      }
    } 
  }

  override def receive = enroll orElse notifying orElse isServiceReady orElse mediatorIsUp orElse superviseeIsTerminated orElse unknown

}
