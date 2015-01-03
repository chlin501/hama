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
package org.apache.hama.monitor

import akka.actor.ActorRef
import java.io.DataInput
import java.io.DataOutput
import java.io.IOException
import java.net.InetAddress
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.ArrayWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.MapWritable
import org.apache.hadoop.io.ObjectWritable
import org.apache.hadoop.io.Writable
import org.apache.hama.Event
import org.apache.hama.HamaConfiguration
import org.apache.hama.bsp.v2.Task
import org.apache.hama.conf.Setting
import org.apache.hama.groom.GroomServer
import org.apache.hama.groom.Slot
import org.apache.hama.util.Utils._
import scala.collection.immutable.Queue

trait ProbeMessages
final case class Notification(result: Any) extends ProbeMessages
final case object EmptyProbeMessages extends ProbeMessages
final case object ListService extends ProbeMessages
final case class ServicesAvailable(services: Array[ActorRef])
      extends ProbeMessages
final case class FindServiceBy(name: String) extends ProbeMessages
final case class ServiceAvailable(service: Option[ActorRef]) 
      extends ProbeMessages
final case class SubscribeTo(events: Event*) extends ProbeMessages
final case class GetMetrics(service: ActorRef, msg: Any) extends ProbeMessages
final case object GetGroomStats extends ProbeMessages

object CollectedStats {

  def apply(data: Writable): CollectedStats = data match {
    case null => throw new IllegalArgumentException("Stats is missing!")
    case _ => new CollectedStats(data)
  }

}

/**
 * CollectedStats contains statistics to be collected by a specific collector.
 * @param v is the stats collected.
 */
class CollectedStats(v: Writable) extends Writable with ProbeMessages {

  /* stats data */
  protected[monitor] var value: Writable = v

  def data(): Writable = value

  override def readFields(in: DataInput) {
    value = ObjectWritable.readObject(in, new HamaConfiguration).
                           asInstanceOf[Writable]
  }

  override def write(out: DataOutput) {
    value.write(out)
  }

  override def toString(): String = 
    "CollectedStats("+ data.toString +")"

}

object Stats {

  def apply(dest: String, data: Writable): Stats = dest match {
    case null | "" => 
      throw new IllegalArgumentException("Destination (tracker) is missing!")
    case _ => data match {
      case null => throw new IllegalArgumentException("Stats data is missing!")
      case _ => new Stats(dest, data)
    }
  }

}


/**
 * Stats contains statistics data to be reported to a specific destination, 
 * usually a Tracker.
 * @param d is the destination to which this stats will be sent.
 * @param v is the stats collected.
 */
class Stats(d: String, v: Writable) extends Writable with ProbeMessages {

  /* tracker name */
  protected[monitor] var tracker: Text = new Text(d)

  /* stats data */
  protected[monitor] var value: Writable = v

  def dest(): String = tracker.toString
  def data(): Writable = value

  override def readFields(in: DataInput) {
    tracker = new Text(Text.readString(in))
    value = ObjectWritable.readObject(in, new HamaConfiguration).
                           asInstanceOf[Writable]
  }

  override def write(out: DataOutput) {
    tracker.write(out)
    value.write(out)
  }

  override def toString(): String = "Stats("+ dest +","+ data.toString +")"

}

object GroomStats {  

  def apply(name: String, host: String, port: Int, maxTasks: Int,
            slotStats: SlotStats): GroomStats = {
    val stats = new GroomStats
    stats.n = name
    stats.h = host
    stats.p = port
    stats.mt = maxTasks
    stats.s = slotStats
    stats
  }

  def apply(name: String, host: String, port: Int, maxTasks: Int): 
      GroomStats = {
    val stats = new GroomStats
    stats.n = name
    stats.h = host
    stats.p = port
    stats.mt = maxTasks
    stats.s = SlotStats.defaultSlotStats(Setting.groom)
    stats
  }

}

final class GroomStats extends Writable with ProbeMessages {

  import GroomStats._

  /* groom server's actor name */
  protected[monitor] var n: String = classOf[GroomServer].getSimpleName

  /* host */
  protected[monitor] var h: String = InetAddress.getLocalHost.getHostName

  /* port */
  protected[monitor] var p: Int = 50000 

  /* max tasks */
  protected[monitor] var mt: Int = 3

  /* slots */
  protected[monitor] var s: SlotStats = 
    SlotStats.defaultSlotStats(Setting.groom)
  
  def name(): String = n 

  def host(): String = h 

  def port(): Int = p

  def hostPort(): String = host + ":" + port
  
  def maxTasks(): Int = mt
 
  def slots(): SlotStats = s

  @throws(classOf[IOException])
  override def write(out: DataOutput) {
    Text.writeString(out, name)
    Text.writeString(out, host)
    out.writeInt(port)
    out.writeInt(maxTasks)
    s.write(out)
  }

  @throws(classOf[IOException])
  override def readFields(in: DataInput) {
    n = Text.readString(in)
    h = Text.readString(in)
    p = in.readInt
    mt = in.readInt
    s = SlotStats.defaultSlotStats(Setting.groom)
    s.readFields(in)
  }

  override def equals(o: Any): Boolean = o match {
    case that: GroomStats => that.isInstanceOf[GroomStats] &&
      that.n.equals(n) && that.h.equals(h) && (that.p == p) && 
      (that.mt == mt) && that.s.equals(s) 
    case _ => false
  }
  
  override def hashCode(): Int = 
    41 * ( 
        41 * ( 
          41 * (
            41 * (
              41 + n.toString.hashCode
            ) + h.toString.hashCode
          ) + p
        ) + mt
    ) + s.hashCode
}

object SlotStats {

  val broken = "broken"
  val none = "none"

  def defaultSlotStats(setting: Setting): SlotStats = {
    val nrOfSlots = setting.hama.getInt("bsp.tasks.maximum", 3)
    val slots = (for(seq <- 1 to nrOfSlots) yield "none").toArray
    val crashCount = (for(seq <- 1 to nrOfSlots) yield (seq, 0)).toMap
    val maxRetries = setting.hama.getInt("groom.executor.max_retries", 3)
    SlotStats(slots, crashCount, maxRetries)
  }

  def apply(slots: Array[String], crashCount: Map[Int, Int], 
            maxRetries: Int): SlotStats = {
    val stats = new SlotStats
    stats.ss = toSlots(slots)
    stats.cc = toCrashCount(crashCount)
    stats.mr = maxRetries
    stats
  }

  def toSlots(ss: Array[String]): ArrayWritable = {
    val w = new ArrayWritable(classOf[Text])
    w.set(ss.map{ v => new Text(v)}.asInstanceOf[Array[Writable]])
    w
  }

  def toCrashCount(cc: Map[Int, Int]): MapWritable = {
    val w = new MapWritable
    cc.foreach { case (k, v) => w.put(new IntWritable(k), new IntWritable(v))}  
    w
  }

}

final class SlotStats extends Writable with ProbeMessages {

  import SlotStats._

  /* in 3 state task attempt id, "", and broken */
  protected[monitor] var ss = new ArrayWritable(classOf[Text])

  /* crash count by slot seq */
  protected[monitor] var cc = new MapWritable() // TODO: performance? 

  /* max retries per slot seq */
  protected[monitor] var mr: Int = 3

  def slots(): Array[String] = ss.toStrings

  def crashCount(): Map[Int, Int] = cc.entrySet.toArray.map { o => {
    val e = o.asInstanceOf[java.util.Map.Entry[IntWritable, IntWritable]]
    (e.getKey.get -> e.getValue.get)
  }}.toMap[Int, Int]

  def maxRetries(): Int = mr

  def isBroken(seq: Int): Boolean = if(seq < slots.size) 
    broken.equals(slots()(seq)) else 
    throw new ArrayIndexOutOfBoundsException("Invalid slot seq: "+seq+"!")
  
  @throws(classOf[IOException])
  override def write(out: DataOutput) {
    ss.write(out)
    cc.write(out)
    out.writeInt(mr)
  }

  @throws(classOf[IOException])
  override def readFields(in: DataInput) {
    ss.readFields(in)
    cc.readFields(in)
    mr = in.readInt
  }

  private def mapEquals(target: Map[Int, Int]): Boolean = {
    val source = crashCount
    (source.size == target.takeWhile { e => 
      e._2 == source.getOrElse(e._1, -1) 
    }.size)
  } 

  private def slotsEquals(target: Array[String]): Boolean = {
    val source = slots
    target.takeWhile { e => source.contains(e) }.size == source.size
  }

  override def equals(o: Any): Boolean = o match {
    case that: SlotStats => that.isInstanceOf[SlotStats] &&
      slotsEquals(that.slots) && mapEquals(that.crashCount) && 
      (that.maxRetries == maxRetries) 
    case _ => false
  }

  override def hashCode(): Int = 
    41 * (
      41 * (
        41 + slots.map { e => e.hashCode }.sum
      ) + crashCount.hashCode
    ) + maxRetries

}

object TaskStats {

  def apply(task: Task): TaskStats = {
    val stats = new TaskStats 
    stats.t = task
    stats
  }
  
}

final class TaskStats extends Writable with ProbeMessages {

  protected[monitor] var t: Task = new Task

  def task(): Task = t

  @throws(classOf[IOException])
  override def write(out: DataOutput) {
    t.write(out)
  }

  @throws(classOf[IOException])
  override def readFields(in: DataInput) {
    t.readFields(in) 
  } 

}

object Publish {

  def apply(task: Task): Publish = {
    val pub = new Publish 
    pub.t = task
    pub
  }
  
}

final class Publish extends Writable with ProbeMessages {

  protected[monitor] var t: Task = new Task

  def task(): Task = t

  @throws(classOf[IOException])
  override def write(out: DataOutput) {
    t.write(out)
  }

  @throws(classOf[IOException])
  override def readFields(in: DataInput) {
    t.readFields(in) 
  } 

}
