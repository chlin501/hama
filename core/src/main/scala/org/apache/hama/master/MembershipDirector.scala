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
import akka.actor.Address
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.InitialStateAsEvents 
import akka.cluster.ClusterEvent.MemberUp
import akka.cluster.ClusterEvent.MemberRemoved
import akka.cluster.ClusterEvent.MemberEvent
import org.apache.hama.LocalService
import org.apache.hama.SystemInfo
import scala.collection.immutable.IndexedSeq

trait MembershipDirector extends LocalService {
  
  protected val cluster = Cluster(context.system)

  protected var grooms = Set.empty[ActorRef]

  protected def join(nodes: IndexedSeq[SystemInfo]) = cluster.joinSeedNodes(
    nodes.map { (info) => {
      Address(info.getProtocol.toString, info.getActorSystemName, info.getHost,
              info.getPort)
    }}
  )

  protected def subscribe(stakeholder: ActorRef) =
    cluster.subscribe(stakeholder, initialStateMode = InitialStateAsEvents,
                      classOf[MemberEvent])
 
  protected def unsubscribe(stakeholder: ActorRef) = 
    cluster.unsubscribe(stakeholder)

  def membership: Receive = {
    //case GroomRegistration => 
    // TODO: register to var grooms
    //       e.g. register(sender)
    case MemberUp(member) => LOG.debug("Member is Up: {}", member.address)
    case MemberRemoved(member, previousStatus) => {
      LOG.debug("Member is Removed: {} after {}", member.address, 
                previousStatus)
      // TODO: remove from cache function
      //       e.g. remove(member.address) from var grooms
    }
    case event: MemberEvent => LOG.debug("Rest membership event {}", event)
  }

}
