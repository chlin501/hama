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

private[master] case class Groom(groom: ActorRef, spec: GroomServerSpec) 

/**
 * A service that manages a set of {@link org.apache.hama.groom.GroomServer}s.
 * @param conf contains specific configuration for this service. 
 */
class GroomManager(conf: HamaConfiguration) extends LocalService {

  type GroomHostName = String
  type CrashCount = Int

  /**
   * Store the GroomServerSpec information so that decision can be made during
   * evaluation process.
   */
  private[this] var grooms = Set.empty[Groom] // TODO: move state out of master

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
    mediator ! Request("sched", RescheduleTasks(spec))
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

  override def receive = {
    isServiceReady orElse serverIsUp orElse
    ({case groomSpec: GroomServerSpec => { // register
      LOG.info("{} requests to register {}.", 
               sender.path.name, groomSpec.getName) 
      checkIfRejoin(sender, groomSpec)
      context.watch(sender) // watch remote
     }}: Receive) orElse superviseeIsTerminated orElse unknown
  }
}
