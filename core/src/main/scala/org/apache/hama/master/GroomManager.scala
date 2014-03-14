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

//private[master] case class Groom(ref: ActorRef, spec: GroomServerSpec) 

/**
 * A service that manages a set of {@link org.apache.hama.groom.GroomServer}s.
 * @param conf contains specific configuration for this service. 
 */
class GroomManager(conf: HamaConfiguration) extends LocalService {

  //var mapping = Map.empty[String, Groom]

  var sched: ActorRef = _
  var cancellable: Cancellable = _
  val schedPath: String = "/user/bspmaster/sched"

  override def configuration: HamaConfiguration = conf

  override def name: String = "groomManager"

/*
  def ask {
    context.system.actorSelection(schedPath) ! Identify(schedPath)
    import context.dispatcher
    cancellable = context.system.scheduler.schedule(0.seconds, 3.seconds, self, 
                                                    Timeout("sched"))
  } 

  override def initializeServices {
    // send message asking scheduler to reply
  }

  def isSchedReady: Receive = {
    case ActorIdentity(`schedPath`, Some(ref)) => {
      sched = ref
      cancellable.cancel
    }
    case ActorIdentity(`schedPath`, None) => {
      LOG.warning("Scheduler is still not yet available!")      
    }
  }

  def timeout: Receive = {
    case Timeout(who) => ask  
  }
*/

  /**
   * Notified when the remote actor is offline.
   * Trigger resched event.
  def isTerminated: Receive = {
    case Terminated => {
      LOG.info("{} is terminated! Reschedule all related tasks.", 
               sender.path.name)
      // get scheduler 
      // send resched message! 
    }
  }

  override def isServiceReady: Receive = {
    case IsServiceReady => {
      // check if schduler is up
      // send Load message back
    }
  }
   */

  override def receive = {
    isServiceReady orElse 
    ({case groomSpec: GroomServerSpec => {
      //mapping ++= Map(groomSpec.name -> Groom(sender, groomSpec))
      LOG.info("GroomServer {} now registers from {}.", 
               groomSpec.getName, sender.path.name) 
      // register to supervisor 
      context.watch(sender)
      // wait for notification if the groom fails
      // then raise resched message
     }}: Receive) /*orElse isSchedReady orElse timeout orElse isTerminated*/ orElse unknown
  }
}
