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
import akka.cluster.ClusterEvent.MemberEvent
import akka.cluster.ClusterEvent.MemberRemoved
import akka.cluster.ClusterEvent.MemberUp
import org.apache.hama.Agent
import org.apache.hama.Membership
import org.apache.hama.SubscribeEvent
import org.apache.hama.SystemInfo
import org.apache.hama.groom.GroomStatsReportEvent
import org.apache.hama.groom.GroomRegistration
import org.apache.hama.groom.GroomRequestTaskEvent
import org.apache.hama.groom.GroomTaskFailureEvent
import org.apache.hama.util.Utils._
import scala.collection.immutable.IndexedSeq

final case class GroomJoin(name: String, host: String, port: Int)
final case class GroomLeave(name: String, host: String, port: Int)

trait MembershipDirector extends Membership with Agent { 

  protected lazy val cluster = Cluster(context.system)

  protected var grooms = Set.empty[ActorRef]

  override def join(nodes: IndexedSeq[SystemInfo]) = cluster.joinSeedNodes(
    nodes.map { info => Address(info.getProtocol.toString, 
                                info.getActorSystemName, info.getHost, 
                                info.getPort) }
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

  /**
   * A groom server enrolls.
   */
  protected def enroll(participant: ActorRef) = {
    val groomName = participant.path.name
    val groomHost = participant.path.address.host.getOrElse(null)
    val groomPort = participant.path.address.port.getOrElse(-1)
    if(null == groomHost || -1 == groomPort)
      throw new RuntimeException("Groom "+groomName+" shouldn't come from "+
                                 groomHost+":"+groomPort+"!")
    groomJoin(groomName, groomHost, groomPort) 
    grooms += participant
    LOG.info("Groom server {} enrolls!", participant.path.name)
    participant ! SubscribeEvent(GroomStatsReportEvent, GroomRequestTaskEvent,
                                 GroomTaskFailureEvent)
    LOG.debug("Listening to groom stats report, groom request task, and "+
              "groom task failure events!") 
  }

  protected def groomJoin(name: String, host: String, port: Int) { }

  protected def disenroll(info: SystemInfo) = grooms.find( groom => {
    info.equals(from(groom.path.address))
  }) match {
    case Some(found) => {
      LOG.info("{} leaves!", found.path.name)
      val groomName = found.path.name
      val groomHost = found.path.address.host.getOrElse(null)
      val groomPort = found.path.address.port.getOrElse(-1)
      groomLeave(groomName, groomHost, groomPort) 
      grooms -= found
    }
    case None => 
  } 

  // TODO: remove name because master services talk to groom through bsp master.
  protected def groomLeave(name: String, host: String, port: Int) { }

  protected def memberEvent(event: MemberEvent) { }

}
