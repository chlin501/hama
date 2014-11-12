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
import akka.cluster.ClusterEvent.MemberUp
import akka.cluster.ClusterEvent.MemberEvent
import akka.cluster.Member
import org.apache.hama.Membership
import org.apache.hama.ProxyInfo
import org.apache.hama.SystemInfo
import org.apache.hama.util.Utils._
import scala.collection.immutable.IndexedSeq

final case object GroomRegistration

trait MembershipParticipant extends Membership with org.apache.hama.logging.ActorLog { this: Actor => 

  protected var master = select

  override def join(nodes: IndexedSeq[SystemInfo]): Unit= cluster.joinSeedNodes(
    nodes.map { (info) => {
      Address(info.getProtocol.toString, info.getActorSystemName, info.getHost,
              info.getPort)
    }}
  )

  /**
   * Join master found in ZooKeeper.
   */
  protected def join(): Unit = join(IndexedSeq[SystemInfo](master))

  override def subscribe(stakeholder: ActorRef) =
    cluster.subscribe(stakeholder, classOf[MemberUp])
 
  override def unsubscribe(stakeholder: ActorRef) = 
    cluster.unsubscribe(stakeholder)

  protected def membership: Receive = {
    case MemberUp(member) => whenMemberUp(member)
    case event: MemberEvent => memberEvent(event)
  }

  protected def masterFinder(): MasterFinder

  protected def select(): ProxyInfo = {
    val masters = masterFinder.masters
    if(1 != masters.size) 
      throw new RuntimeException("Master size is not 1, but "+masters.size+"!")
    masters(0)
  }

  protected def whenMemberUp(member: Member) = if(member.hasRole("master")) {
    register(master)
  }

  protected def register(info: ProxyInfo) = 
    context.actorSelection(actorPath(info)) ! GroomRegistration

  protected def memberEvent(event: MemberEvent) { }

}
