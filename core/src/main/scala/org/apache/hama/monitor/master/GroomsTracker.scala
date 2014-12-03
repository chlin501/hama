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

import org.apache.hadoop.io.Writable
import org.apache.hama.monitor.Tracker
import org.apache.hama.monitor.ProbeMessages
import org.apache.hama.monitor.GroomStats
import org.apache.hama.util.Utils._

final case class GetMaxTasks(jobId: String) extends ProbeMessages
final case class GetGroomCapacity(host: String, port: Int) extends ProbeMessages
final case class GroomCapacity(host: String, port: Int, freeSlots: Int) 
      extends ProbeMessages
final case class TotalMaxTasks(jobId: String, allowed: Int) 
     
       extends ProbeMessages

final class GroomsTracker extends Tracker {

  type Groom = String
  type FreeSlots = Int

  private var allStats = Set.empty[GroomStats]
  private val NULL = nullString

  /* total tasks of all groom servers */
  private var totalMaxTasks: Int = 0
  
  /* free slots per groom */
  private var freeSlotsByGroom = Map.empty[Groom, FreeSlots] 

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
    freeSlotsByGroom ++= Map(key(stats) -> freeSlots(stats))

  /**
   * Sum up free slots of a particular GroomStats. 
   * @param stats is groom stats to be calculated.
   * @return sum up free slots of a groom currently has.
   */ 
  private def freeSlots(stats: GroomStats): Int = 
    stats.slots.map { slot => slot match {  
      case NULL => 1 
      case _ => 0 
    }}.sum

  override def groomLeaves(name: String, host: String, port: Int) = 
    allStats.find( stats => 
      stats.name.equals(name) && stats.host.equals(host) && 
      stats.port.equals(port)
    ). map { stats => {
      remove(stats)
      subMaxTasks(stats)
      subFreeSlots(stats)
    }}

  private def remove(stats: GroomStats) = allStats -= stats 

  private def subMaxTasks(stats: GroomStats) = totalMaxTasks -= stats.maxTasks

  private def subFreeSlots(stats: GroomStats) = freeSlotsByGroom -= key(stats)

  private def key(stats: GroomStats): String = 
    stats.name+"_"+stats.host+"_"+stats.port

  override def askFor(action: Any, from: String) = action match {
    /**
     * Ask max task allowed of the entire groom servers.
     */
    case GetMaxTasks(jobId) => inform(from, TotalMaxTasks(jobId, totalMaxTasks))
    /**
     * Check free slot capacity of a particular groom, based on host and port.  
     * @param host is the target groom server name.
     * @param port used by the groom server.
     */
    case GetGroomCapacity(host, port) => allStats.find( stats => 
      host.equals(stats.host) && (port == stats.port)
    ) match {
      case Some(stats) => inform(from, GroomCapacity(host, port, 
        freeSlotsByGroom.get(key(stats)).getOrElse(0)))
      case None => inform(from, GroomCapacity(host, port, 0))
    }
    case _ => LOG.warning("Unknown action {} from {}!", action, from)
  }

}
