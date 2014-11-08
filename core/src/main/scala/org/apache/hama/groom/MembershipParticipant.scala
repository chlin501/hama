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
package org.apache.hama.groom

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Address
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.MemberUp
import akka.cluster.ClusterEvent.MemberEvent
import akka.cluster.Member
import org.apache.hama.Membership
import org.apache.hama.SystemInfo
import scala.collection.immutable.IndexedSeq

trait MembershipParticipant extends Membership { this: Actor => 

  override def join(nodes: IndexedSeq[SystemInfo]) = cluster.joinSeedNodes(
    nodes.map { (info) => {
      Address(info.getProtocol.toString, info.getActorSystemName, info.getHost,
              info.getPort)
    }}
  )

  override def subscribe(stakeholder: ActorRef) =
    cluster.subscribe(stakeholder, classOf[MemberUp])
 
  override def unsubscribe(stakeholder: ActorRef) = 
    cluster.unsubscribe(stakeholder)

  protected def membership: Receive = {
    case MemberUp(member) => whenMemberUp(member)
    case event: MemberEvent => memberEvent(event)
  }

  protected def whenMemberUp(member: Member) = if(member.hasRole("master")) {
    val addr = member.address
    val host = addr.host.getOrElse(null)
    if(null == host) 
      throw new RuntimeException("Master's host value is empty!")
    val port = addr.port.getOrElse(-2)
    if(-2 == port)
      throw new RuntimeException("Invalid master's port value!")
    register(new SystemInfo(addr.protocol, addr.system, host, port))
  }

  protected def register(info: SystemInfo) { }

  protected def memberEvent(event: MemberEvent) { }

}
