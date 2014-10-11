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
package org.apache.hama.bsp.v2

import akka.actor.ActorRef
import org.apache.hadoop.io.Writable
import org.apache.hama.Agent

sealed trait SuperstepMessage
final case class Setup(peer: BSPPeer) extends SuperstepMessage
final case class SetVariables(variables: Map[String, Writable]) 
      extends SuperstepMessage
final case object GetVariables extends SuperstepMessage
final case class Variables(variables: Map[String, Writable]) 
      extends SuperstepMessage
final case class Compute(peer: BSPPeer) extends SuperstepMessage
final case class NextSuperstepClass(next: Class[_]) extends SuperstepMessage
final case class Cleanup(peer: BSPPeer) extends SuperstepMessage

class SuperstepWorker(superstep: Superstep, coordinator: ActorRef) 
      extends Agent {

  protected def setup: Receive = {
    case Setup(peer) => superstep.setup(peer)
  }

  protected def setVariables: Receive = {
    case SetVariables(variables) => superstep.setVariables(variables)
  }

  protected def getVariables: Receive = {
    case GetVariables => Variables(superstep.getVariables)
  }

  protected def compute: Receive = {
    case Compute(peer) => {
      superstep.compute(peer)
      coordinator ! NextSuperstepClass(superstep.next)
    }
  }

  protected def cleanup: Receive = {
    case Cleanup(peer) => superstep.cleanup(peer)
  }

  override def receive = setup orElse setVariables orElse getVariables orElse compute orElse cleanup orElse unknown

}
