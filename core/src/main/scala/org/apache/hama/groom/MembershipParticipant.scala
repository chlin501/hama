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

import akka.actor.ActorRef
import akka.actor.Address
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.MemberEvent
import akka.cluster.ClusterEvent.MemberRemoved
import akka.cluster.ClusterEvent.MemberUp
import akka.cluster.ClusterEvent.CurrentClusterState
import akka.cluster.Member
import org.apache.hama.RemoteService
import org.apache.hama.Membership
import org.apache.hama.ProxyInfo
import org.apache.hama.SystemInfo
import org.apache.hama.util.MasterDiscovery
import org.apache.hama.util.Utils._
import scala.collection.immutable.IndexedSeq

final case object GroomRegistration

trait MembershipParticipant extends Membership with MasterDiscovery { 
 
  this: RemoteService => 

  protected lazy val cluster = Cluster(context.system)

  override def join(nodes: IndexedSeq[SystemInfo]): Unit= cluster.joinSeedNodes(
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
    case MemberRemoved(member, prevStatus) => if(member.hasRole("master"))
      shutdown
    case event: MemberEvent => memberEvent(event)
    case CurrentClusterState(members, unreachable, seenBy, leader, 
                             roleLeaderMap) => 
  }

  protected def whenMemberUp(member: Member) = if(member.hasRole("master")) {
    master.map { (m) => register(m) }
  }

  protected def register(target: ProxyInfo) = 
    findProxyBy(target.getActorName) match { 
      case Some(proxy) => proxy ! GroomRegistration
      case None => LOG.warning("Master not found with {}!", target.getActorName)
    }

  protected def memberEvent(event: MemberEvent) { }

  override def afterLinked(target: String, proxy: ActorRef): Unit = 
    master.map { m => target.equals(m.getActorName) match {
      case true => {
        join(IndexedSeq[SystemInfo](m)) 
        subscribe(self)
      }
      case false =>
    }}

}
