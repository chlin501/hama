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
package org.apache.hama.monitor.master

import akka.actor.ActorRef
import org.apache.hadoop.io.Writable
import org.apache.hama.bsp.BSPJobID
import org.apache.hama.master.GroomLeave
import org.apache.hama.master.GroomLeaveEvent
import org.apache.hama.monitor.Tracker
import org.apache.hama.monitor.ProbeMessage
import org.apache.hama.monitor.GroomStats
import org.apache.hama.monitor.SlotStats
import org.apache.hama.util.Utils._

final case class ClientMaxTasksAllowed(jobId: BSPJobID) extends ProbeMessage
final case class ClientTasksAllowed(jobId: BSPJobID, maxTasks: Int) 
      extends ProbeMessage
final case class GetMaxTasks(jobId: String) extends ProbeMessage
final case class GetGroomCapacity(grooms: Array[ActorRef]) extends ProbeMessage
final case class GroomCapacity(mapping: Map[ActorRef, Int]) 
      extends ProbeMessage
final case class TotalMaxTasks(jobId: String, allowed: Int) 
      extends ProbeMessage

object GroomsTracker {

  def fullName(): String = classOf[GroomsTracker].getName
}

final class GroomsTracker extends Tracker {

  type Groom = String
  type FreeSlots = Int

  private var allStats = Set.empty[GroomStats]

  /* total tasks of all groom servers */
  private var totalMaxTasks: Int = 0
  
  /* free slots per groom */
  private var freeSlotsPerGroom = Map.empty[Groom, FreeSlots] 

  override def initialize() = subscribe(GroomLeaveEvent)

  override def receive(stats: Writable) = stats match { 
    case stats: GroomStats => {
      update(stats)
      sumMaxTasks(stats)
      sumFreeSlots(stats)
    }
    case other@_ => LOG.warning("Unknown stats data: {}", other)
  }

  private def update(stats: GroomStats) = allStats ++= Set(stats)

  private def sumMaxTasks(stats: GroomStats) = totalMaxTasks += stats.maxTasks

  private def sumFreeSlots(stats: GroomStats) =  
    freeSlotsPerGroom ++= Map(key(stats) -> freeSlots(stats))

  /**
   * Sum up free slots of a particular GroomStats. 
   * @param stats is groom stats to be calculated.
   * @return sum up free slots of a groom currently has.
   */ 
  private def freeSlots(stats: GroomStats): Int = stats.slots.slots.map {  
    case SlotStats.none => 1 
    case _ => 0 
  }.sum

  /**
   * Get notified when some events happended.
   */
  override def notified(event: Any) = event match {
    case GroomLeave(name, host, port) => allStats.find( stats => 
      stats.name.equals(name) && stats.host.equals(host) && 
      stats.port.equals(port)
    ). map { stats => {
      remove(stats)
      subMaxTasks(stats)
      subFreeSlots(stats)
    }}
    case _ => LOG.warning("Unknown event {}!", event)
  }

  private def remove(stats: GroomStats) = allStats -= stats 

  private def subMaxTasks(stats: GroomStats) = totalMaxTasks -= stats.maxTasks

  private def subFreeSlots(stats: GroomStats) = freeSlotsPerGroom -= key(stats)

  private def key(stats: GroomStats): String = 
    stats.name+"_"+stats.host+"_"+stats.port

  /**
   * Response to request from servcies.
   */
  override def askFor(action: Any, from: ActorRef) = action match {
    /**
     * Ask max task allowed of the entire groom servers.
     */
    case GetMaxTasks(jobId) => from ! TotalMaxTasks(jobId, totalMaxTasks) 
    /**
     * Check free slot capacity of a particular groom, based on host and port.  
     * @param host is the target groom server name.
     * @param port used by the groom server.
     */
    case GetGroomCapacity(grooms: Array[ActorRef]) => 
      from ! GroomCapacity(findFreeSlotsFor(grooms))
    case ClientMaxTasksAllowed(jobId) => 
      from ! ClientTasksAllowed(jobId, totalMaxTasks)
    case _ => LOG.warning("Unknown action {} from {}!", action, from)
  }

  private def findFreeSlotsFor(grooms: Array[ActorRef]): Map[ActorRef, Int] = 
    grooms.map { groom => {
      val host = groom.path.address.host.getOrElse(null)
      val port = groom.path.address.port.getOrElse(-1)
      findGroomStatsBy(host, port) match {
        case Some(stats) => 
          (groom -> freeSlotsPerGroom.get(key(stats)).getOrElse(0))
        case None => (groom -> 0)
      }
    }}.toMap

  private def findGroomStatsBy(host: String, port: Int): Option[GroomStats] = 
    allStats.find( stats => stats.host.equals(host) && (port == stats.port)) 

/*
    match {
      case Some(stats) => from ! GroomCapacity(host, port, 
        freeSlotsPerGroom.get(key(stats)).getOrElse(0))
      case None => from ! GroomCapacity(host, port, 0) 
    }
*/

}
