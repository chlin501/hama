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
import org.apache.hama._
import org.apache.hama.bsp.v2.GroomServerSpec
import org.apache.hama.master._
import scala.concurrent.duration._
import scala.collection.immutable.Queue

private[master] final case class Groom(groom: ActorRef, spec: GroomServerSpec) 
private[master] final case object Dequeue

final case class TotalTaskCapacity(maxTasks: Int)
final case class Locate(spec: GroomServerSpec)

/**
 * A service that manages a set of {@link org.apache.hama.groom.GroomServer}s.
 * @param conf contains specific configuration for this service. 
 */
class GroomManager(conf: HamaConfiguration) extends LocalService {

  type GroomHostName = String
  type CrashCount = Int

  private var specQueue = Queue[GroomServerSpec]()
  private var specQueueWatcher: Cancellable = _

  /**
   * Store the GroomServerSpec information so that decision can be made during
   * evaluation process.
   */
  // TODO: move state out of master
  private[this] var grooms = Set.empty[Groom] 

  /**
   * Identical GroomServer host name logically represents the same GroomServer, 
   * even if the underlying hardware is changed.
   */
  // TODO: may need to reset crash count or 
  //       store more offline line grooms stat info.
  private[this] var offlineGroomsStat = Map.empty[GroomHostName, CrashCount] 

  override def configuration: HamaConfiguration = conf

  override def name: String = "groomManager"

  /**
   * Quarantine offline GroomServer.
   */
  def quarantine(offline: ActorRef, resched:(GroomServerSpec) => Unit) {
    grooms.find(p=>p.groom.equals(offline)) match { // move to offline grooms
      case Some(groom) => {
        grooms -= groom
        offlineGroomsStat.get(groom.spec.getName) match {
          case Some(crashCount) => 
            offlineGroomsStat = offlineGroomsStat.mapValues{ cnt => cnt + 1 }
          case None => offlineGroomsStat ++= Map(groom.spec.getName -> 1)
        }
        resched(groom.spec) 
      }
      case None => 
        LOG.warning("GroomServer {} is watched but not found in the list!")
    }
    LOG.info("OfflineGroomsStat: {}", offlineGroomsStat.mkString(", "))
  }

  def rescheduleTasks(spec: GroomServerSpec) {
    if(null != mediator) 
      mediator ! Request("sched", RescheduleTasks(spec))
    else 
      LOG.warning("Master is not ready so rescheduling is not possible!")
  }

  override def offline(groom: ActorRef) {
    quarantine(groom, rescheduleTasks)
  }

  def checkIfRejoin(from: ActorRef, spec: GroomServerSpec) {
    grooms ++= Set(Groom(from, spec))
    // TODO: 
    // 1. specific stat info recording groom crash info.
    // 2. if spec is with refresh hardware, reset crashed count to 0
  }
  
  override def afterMediatorUp {
    import context.dispatcher
    specQueueWatcher = 
      context.system.scheduler.schedule(0.seconds, 5.seconds, self, 
                                        Dequeue)
  }

  def register: Receive = {
    case groomSpec: GroomServerSpec => { 
      LOG.info("{} requests to register {}.", 
               sender.path.name, groomSpec.getName) 
      checkIfRejoin(sender, groomSpec)
      specQueue = specQueue.enqueue(groomSpec)
      context.watch(sender) // watch remote
    }
  }

  /**
   * Evaluate cluster capacity.
   */
  def evaluate(spec: GroomServerSpec) {
    mediator ! Request("curator", TotalTaskCapacity(spec.getMaxTasks))
  }

  /**
   * Link remote TaskManager for scheduling tasks.
   */
  def locate(spec: GroomServerSpec)  {
    mediator ! Request("sched", Locate(spec))
  }

  def dequeue: Receive = {
    case Dequeue => {
      while(!specQueue.isEmpty) {
        val (spec, rest) = specQueue.dequeue
        evaluate(spec)
        locate(spec)
        specQueue = rest
      }
    } 
  }

  override def receive = isServiceReady orElse serverIsUp orElse register orElse superviseeIsTerminated orElse unknown

}
