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
import org.apache.hama.HamaConfiguration
//import org.apache.hama.groom.GroomServerStat
import org.apache.hama.monitor.Tracker
import org.apache.hama.monitor.GroomStats

/**
 * Ask {@link GroomsTracker} for corresponded GroomServerStat(s).
 * @param groomServers is the target to which tasks will be scheduled.
 * @param from denotes who sends this request.
final case class AskGroomServerStat(groomServers: Array[String],
                                    from: ActorRef)
 */

final class GroomsTracker extends Tracker {

  private var allStats = Set.empty[GroomStats]

  //private var // calculated stats e.g. total max tasks, etc. fields

  override def initialize() { }

/*
  def groomStats: Receive = {
    case stats: GroomStats => {
      // TODO: sum up related data
    }
  }

  //def groomLeave/ JoinEvent: Receive = {
    //case GroomLeave(name, host, port)/ GroomJoin(name, host, port)
  //}
*/
 
/*
  private var groomTasksStat = Set.empty[GroomServerStat]

   * Receive {@link GroomServerStat} report from {@link GroomReporter}.
  private def renewGroomServerStat: Receive = {
    case stat: GroomServerStat => groomTasksStat ++= Set(stat)
  }
  
   * Find corresponded {@link GroomServerStat}. 
  private def askGroomServerStat: Receive = {
    case AskGroomServerStat(grooms, from) => {
      var stats = Set.empty[GroomServerStat]  
      grooms.foreach( groom => {
        groomTasksStat.filter( stat => stat.getName.equals(groom)) match {
          case filtered: Set[GroomServerStat] => stats ++= Set(filtered.head)
          case unknown@_ => LOG.warning("No stat found for GroomServer {}", 
                                        groom)
        }
      })
      if(!stats.isEmpty) from ! stats 
      else LOG.warning("{} No GroomServerStat found!", grooms.mkString(", "))
    }
  }

  override def receive = renewGroomServerStat orElse askGroomServerStat orElse unknown
*/
}
