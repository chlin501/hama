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

  var grooms = Set.empty[Groom] // TODO: move state out of master
  var offlineGrooms = Set.empty[Groom]

  override def configuration: HamaConfiguration = conf

  override def name: String = "groomManager"

  /**
   * Quarantine offline GroomServer.
   */
  def quarantine(offline: ActorRef, resched:(GroomServerSpec) => Unit) {
    grooms.find(p=>p.groom.equals(offline)) match { // move to offline grooms
      case Some(found) => {
        grooms -= found
        offlineGrooms += found
        resched(found.spec) 
      }
      case None => 
        LOG.warning("GroomServer {} is watched but not found in list!")
    }
  }

  def rescheduleTasks(spec: GroomServerSpec) {
    mediator ! Request("sched", RescheduleTasks(spec))
  }

  override def offline(groom: ActorRef) {
    quarantine(groom, rescheduleTasks)
  }

  override def receive = {
    isServiceReady orElse serverIsUp orElse
    ({case groomSpec: GroomServerSpec => { // register
      grooms ++= Set(Groom(sender, groomSpec))
      LOG.info("{} requests to register {}.", 
               sender.path.name, groomSpec.getName) 
      context.watch(sender) // watch remote
     }}: Receive) orElse superviseeIsTerminated orElse unknown
  }
}
