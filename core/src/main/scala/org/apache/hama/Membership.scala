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
package org.apache.hama

import akka.actor.Actor
import akka.actor.ActorRef
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.InitialStateAsEvents 
import akka.cluster.ClusterEvent.MemberEvent
import akka.cluster.ClusterEvent.MemberRemoved
import akka.cluster.ClusterEvent.MemberUp
import akka.cluster.Member
import akka.cluster.MemberStatus
import scala.collection.immutable.IndexedSeq

trait Membership { this: Actor =>
  
  protected val cluster = Cluster(context.system)

  def join(nodes: IndexedSeq[SystemInfo])

  def subscribe(stakeholder: ActorRef) 
 
  def unsubscribe(stakeholder: ActorRef) 

/*
  def membership: Receive = {
    case MemberUp(member) => register(member)
    case MemberRemoved(member, prevStatus) => deregister(member, prevStatus)
    case event: MemberEvent => memberEvent(event)
  }

  def register(member: Member) { }

  def deregister(member: Member, prevStatus: MemberStatus) { }

  def memberEvent(event: MemberEvent) { }
*/

}
