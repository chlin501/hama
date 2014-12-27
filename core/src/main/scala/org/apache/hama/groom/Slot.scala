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
import org.apache.hama.bsp.TaskAttemptID
import org.apache.hama.logging.CommonLog
import org.apache.hama.monitor.SlotStats

object Slot {

  /**
   * Create a slot marked as broken.
   */
  def apply(seq: Int): Slot = {
    val broken = new Broken
    broken.s = seq
    broken
  }

  /**
   * An initial state slot.   
   */
  def emptySlot(seq: Int): Slot = Slot(seq, None, "", None, None)

  def apply(seq: Int, taskAttemptId: Option[TaskAttemptID], master: String,
            executor: Option[ActorRef], container: Option[ActorRef]): Slot = {
    require( 0 < seq, "Seq value should be larger than 0, but "+seq+" found!")
    require( null != master && !"".equals(master), 
            "Master string can't be empty!")
    val seat = new Seat
    seat.s = seq
    seat.id = taskAttemptId
    seat.m = master
    seat.e = executor
    seat.c = container
    seat 
  }

  def apply(seq: Int, taskAttemptId: TaskAttemptID, master: String, 
            executor: ActorRef, container: ActorRef): Slot = 
    apply(seq, Option(taskAttemptId), master, Option(executor), 
          Option(container)) 
  
}

sealed trait Slot {

  def seq(): Int

  def taskAttemptId(): Option[TaskAttemptID]

  def master(): String 

  def executor(): Option[ActorRef]

  def container(): Option[ActorRef]

}

final class Seat extends Slot {

  protected[groom] var s: Int = -1

  protected[groom] var id: Option[TaskAttemptID] = None

  protected[groom] var m: String = ""

  protected[groom] var e: Option[ActorRef] = None

  protected[groom] var c: Option[ActorRef] = None

  def seq(): Int = s 

  def taskAttemptId(): Option[TaskAttemptID] = id

  def master(): String = m

  def executor(): Option[ActorRef] = e

  def container(): Option[ActorRef] = c

}

final class Broken extends Slot {

  protected[groom] var s: Int = -1

  def seq(): Int = s

  def taskAttemptId(): Option[TaskAttemptID] = None

  def master(): String = "" 

  def executor(): Option[ActorRef] = None

  def container(): Option[ActorRef] = None

}

object SlotManager {

  def apply(maxTasks: Int): SlotManager = {
    val manager = new SlotManager
    manager.initialize(maxTasks) 
    manager
  }
}

class SlotManager extends CommonLog {

  import Slot._

  /**
   * The max size of slots can't exceed configured maxTasks.
   */
  protected[groom] var slots = Set.empty[Slot]

  /**
   * Initialize slots with default slots value to 3, which comes from maxTasks,
   * or "bsp.tasks.maximum".
   * @param constraint of the slots can be created.
   */
  private def initialize(constraint: Int = 3) {
    slots = (for(seq <- 1 to constraint) yield Set(emptySlot(seq))).flatten.
            toSet
    LOG.info("{} GroomServer slots are initialied.", constraint)
  }

  /**
   * Translate task attempt id or broken slot into literal string.
   */
  protected[groom] def taskAttemptIdStrings(): Array[String] = 
    slots.map { slot => isSlotDefunct(slot.seq) match {
      case true => SlotStats.broken
      case false => slot.taskAttemptId match {
        case Some(taskAttemptId) => taskAttemptId.toString
        case None => SlotStats.none
      }
    }}.toArray

  /**
   * Book slot with corresponded container and task attempt id. This function is
   * executed after executor is ready, so executor comes from slot having the.
   * same seq.
   */
  protected[groom] def book(slotSeq: Int, taskAttemptId: TaskAttemptID, 
                            container: ActorRef) = findThenMap[Unit]({ slot => 
    (slotSeq == slot.seq) })({ slot => slot.taskAttemptId match {
      case None => slotNotOccupied(slot, taskAttemptId, container)
      case Some(id) => 
        LOG.error("Task {} can't be booked at slot {} for other task {} "+
                  "exists!", taskAttemptId, slotSeq, id)
    }
  })

  protected def slotNotOccupied(old: Slot, taskAttemptId: TaskAttemptID,
                                container: ActorRef) =  
    update(old.seq, Option(taskAttemptId), old.master, old.executor, 
           Option(container))

  protected[groom] def update(seq: Int, taskAttemptId: Option[TaskAttemptID],
                              master: String, executor: Option[ActorRef],
                              container: Option[ActorRef]) = 
    findThenMap({ slot => slot.seq == seq})({ found => 
      slots -= found
      val newSlot = Slot(seq, taskAttemptId, master, executor, container)
      slots += newSlot
    })

  protected[groom] def clear(seq: Int) = slots.find( slot =>  
    (slot.seq == seq)
  ) match {
    case Some(found) => update(found.seq, None, found.master, None, None)
    case None => throw new RuntimeException("No matched slot for seq "+seq)
  }

  protected def removeTaskFromSlot(old: Slot) = 
    update(old.seq, None, old.master, old.executor, old.container) 

  /**
   * Clear slot with corresponded slot seq and task attempt id values specified.
   */
  protected[groom] def clearSlotBy(seq: Int, taskAttemptId: TaskAttemptID) = 
    find[Unit]({ slot => 
      (slot.seq == seq) && slot.taskAttemptId.equals(Option(taskAttemptId))
    })({ found => removeTaskFromSlot(found) })({ 
      LOG.error("No matched slot found for seq {} and task attempt id {}", seq, 
                taskAttemptId) 
    })

  /**
   * The number of tasks allowed.
   */
  protected[groom] def maxTasksAllowed(): Int = slots.size 

  protected[groom] def findSlotBy(seq: Int): Option[Slot] = find({ slot =>
    slot.seq == seq
  })

  protected[groom] def findSlotBy(taskAttemptId: TaskAttemptID): Option[Slot] =
    find({ slot => slot.taskAttemptId.equals(Option(taskAttemptId)) })

  protected[groom] def find(cond: (Slot) => Boolean): Option[Slot] = 
    slots.find( slot => cond(slot))

  protected[groom] def find[A <: Any](cond: (Slot) => Boolean)
                                     (action: (Slot) => A)
                                     (f: => A): A =
    slots.find( slot => cond(slot)) match {
      case Some(found) => action(found)
      case None => f
    }

  protected[groom] def findThenMap[A <: Any](cond: (Slot) => Boolean)
                                            (action: (Slot) => A): 
    Option[A] = slots.find(cond).map { found => action(found) }

  protected[groom] def defunct(seq: Int) = 
    findThenMap({ slot => (slot.seq == seq) })({ found => { 
      slots -= found
      slots += Slot(seq) 
    }})

  protected[groom] def isSlotDefunct(seq: Int): Boolean = slots.find( slot => 
    (slot.seq == seq)) match {
    case Some(found) => found.isInstanceOf[Broken]
    case None => { 
      LOG.error("No matched seq {} for defunct slot!", seq) 
      false
    }
  }

  protected[groom] def markAsBroken(seq: Int) = findSlotBy(seq) match {
    case Some(found) => {
      slots -= found
      slots += Slot(seq) 
    }
    case None => LOG.error("No matched seq for slot {}!", seq)
  }

  protected[groom] def numSlotsOccupied(): Int = slots.count( slot => 
    !None.equals(slot.taskAttemptId)
  )

  protected[groom] def nonBrokenSlotsOccupied(): Int = slots.count( slot => 
    !isSlotDefunct(slot.seq) && !None.equals(slot.taskAttemptId) 
  )

}

 
