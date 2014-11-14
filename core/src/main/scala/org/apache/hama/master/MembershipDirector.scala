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
import org.apache.hama.Agent
import org.apache.hama.Membership
import org.apache.hama.SystemInfo
import org.apache.hama.groom.GroomRegistration
import org.apache.hama.util.Utils._
import scala.collection.immutable.IndexedSeq

trait MembershipDirector extends Membership with Agent { 

  protected val cluster = Cluster(context.system)

  protected var grooms = Set.empty[ActorRef]

  override def join(nodes: IndexedSeq[SystemInfo]) = cluster.joinSeedNodes(
    nodes.map { (info) => {
      Address(info.getProtocol.toString, info.getActorSystemName, info.getHost,
              info.getPort)
    }}
  )

  override def subscribe(stakeholder: ActorRef) =
    cluster.subscribe(stakeholder, initialStateMode = InitialStateAsEvents,
                      classOf[MemberEvent])
 
  override def unsubscribe(stakeholder: ActorRef) = 
    cluster.unsubscribe(stakeholder)

  def membership: Receive = {
    case GroomRegistration => enroll(sender)
    case MemberRemoved(member, prevStatus) => disenroll(from(member.address))
    case event: MemberEvent => memberEvent(event)
  }

  protected def enroll(participant: ActorRef) = {
    LOG.info("{} enrolls to Master!", participant.path.name)
    grooms ++= Set(participant)
  }

  protected def disenroll(info: SystemInfo) = grooms.find( groom => {
    info.equals(from(groom.path.address))
  }) match {
    case Some(found) => {
      LOG.info("{} disenrolls out of Master!", found.path.name)
      grooms -= found
    }
    case None => 
  } 

  protected def memberEvent(event: MemberEvent) { }

}
