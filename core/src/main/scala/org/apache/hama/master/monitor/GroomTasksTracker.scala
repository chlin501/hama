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

  //TODO: need additional fields marking the slots is booked by a 
  //      specific jobId, and the way to unbooked the slot
  def findSlotsAvailable: Receive = {
    case res: Resource => {
      val nextDealer = res.next
      LOG.info("Next dealer is {}", nextDealer)
      val preferredNumBSPTasks = res.job.getNumBSPTasks
      var slotsAllocated = 0 
      var avail = Set.empty[GroomAvailable]
      // note: it may happen that not enough availabe slots in filling
      //       preferredNumBSPTasks after allocation.
      res.available.takeWhile( groom => {
        val filteredSet =
          groomTasksStat.getOrElse(groom.name, Set()).filter( slot => {
            LOG.debug("GroomAvailable.name: {} slot: {}", groom.name, slot)
            None.equals(slot.task)
          }).takeWhile( slot => {
            slotsAllocated += 1
            slotsAllocated < (preferredNumBSPTasks+1)
          }).map { v =>  v.seq }
        LOG.debug("GroomAvailable.name: {} filterdSet: {}",
                  groom.name, filteredSet)
        if(filteredSet.size > 0)
          avail ++= Set(GroomAvailable(groom.name, groom.maxTasks, 
                                       filteredSet.toArray))

        slotsAllocated < (preferredNumBSPTasks+1)
      })  
      if(preferredNumBSPTasks != (slotsAllocated-1)) {
        LOG.debug("{} is configured with {}, but only {} slots allocated.", 
                  res.job.getId, preferredNumBSPTasks, (slotsAllocated-1))
      }
        
    }
  }

  override def receive = {
    isServiceReady orElse updateGroomStat /*orElse findSlotsAvailabe*/ orElse unknown
  } 
}
