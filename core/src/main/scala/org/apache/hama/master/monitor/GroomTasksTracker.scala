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
package org.apache.hama.master.monitor

import org.apache.hama._
import org.apache.hama.groom._
import org.apache.hama.bsp.v2.Task
import org.apache.hama.master._

final class GroomTasksTracker(conf: HamaConfiguration) extends LocalService {

  var groomTasksStat = Map.empty[String, Set[Slot]]

  override def configuration: HamaConfiguration = conf

  override def name: String = "groomTasksTracker"

  def updateGroomStat: Receive = {
    case stat: GroomStat => { 
      groomTasksStat.find(p=> p._1.equals(stat.groomName)) match {
        case Some((key, value)) =>  
          groomTasksStat ++= Map(stat.groomName -> stat.slots)
        case None => 
          groomTasksStat ++= Map(stat.groomName -> stat.slots)
      }
    }
  }

/*
  def findSlotsAvailable: Receive = {
    case res: Resource => {
      val nextDealer = res.next
      LOG.info("Ask monitor ({}) passing resource.", nextDealer)
      var avail = res.available
      avail.foreach(groom => {
        groomTasksStat.get(groom.name) match {
          case Some(slots) => {
            slots.task match {
              case Some(occupied) => 
              case None => {
                 groom.freeSlots Set(slots.seq) 
              }
            }
          }
          case None => avail -= groom.name
        }
      })
    }
  }
*/

  override def receive = {
    isServiceReady orElse updateGroomStat /*orElse findSlotsAvailabe*/ orElse unknown
  } 
}
