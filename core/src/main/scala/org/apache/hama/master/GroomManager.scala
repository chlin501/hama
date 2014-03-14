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

private[master] case class Groom(ref: ActorRef, spec: GroomServerSpec) 

/**
 * A service that manages a set of {@link org.apache.hama.groom.GroomServer}s.
 * @param conf contains specific configuration for this service. 
 */
class GroomManager(conf: HamaConfiguration) extends LocalService {

  var grooms = Set.empty[Groom]

  var scheduler: ActorRef = _

  override def configuration: HamaConfiguration = conf

  override def name: String = "groomManager"

  def quarantine(offline: ActorRef) {
    
  }

  def rescheduleTasks {
  }

  override def offline(target: ActorRef) {
    quarantine(target)
    rescheduleTasks  
  }

  override def receive = {
    case MasterIsUp => { sender ! Request("sched") }
    case Response(sched) => { scheduler = sched }
    ({case groomSpec: GroomServerSpec => { // register
      grooms ++= Set(Groom(sender, groomSpec))
      LOG.info("{} requests to register {}.", 
               sender.path.name, groomSpec.getName) 
      context.watch(sender) // watch remote
     }}: Receive) orElse isServiceReady orElse isTerminated orElse unknown
  }
}
