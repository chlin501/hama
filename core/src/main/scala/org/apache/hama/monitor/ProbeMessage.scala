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
import org.apache.hama.HamaConfiguration
import org.apache.hama.groom.GroomServer
import org.apache.hama.groom.Slot
import org.apache.hama.util.Utils._
import scala.collection.immutable.Queue

trait ProbeMessages
final case object EmptyProbeMessages extends ProbeMessages
final case object ListService extends ProbeMessages
final case class ServicesAvailable(services: Array[String])
      extends ProbeMessages
final case class GetMetrics(service: String, command: Any)
      extends ProbeMessages
final case object GetGroomStats extends ProbeMessages
final case object GetTaskStats extends ProbeMessages

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

// TODO: change slot to slot stats with slot black list, etc.
object GroomStats {  

  def apply(name: String, host: String, port: Int, maxTasks: Int,
            slots: Array[String]): GroomStats = {
    val stats = new GroomStats
    stats.n = name
    stats.h = host
    stats.p = port
    stats.mt = maxTasks
    stats.s = toWritable(slots) 
    stats
  }

  def apply(name: String, host: String, port: Int, maxTasks: Int): 
      GroomStats = {
    val stats = new GroomStats
    stats.n = name
    stats.h = host
    stats.p = port
    stats.mt = maxTasks
    stats.s = defaultSlots(maxTasks)
    stats
  }

  def list(slots: Set[Slot]): Array[String] = 
    slots.map { s => s.taskAttemptId match {
      case None => nullString
      case Some(taskAttemptId) => taskAttemptId.toString
    }}.toArray

  final def toWritable(strings: Array[String]): ArrayWritable = {
    val w = new ArrayWritable(classOf[Text])
    w.set(strings.map { e => new Text(e) })
    w
  }

  final def defaultSlots(maxTasks: Int): ArrayWritable = {
    val w = new ArrayWritable(classOf[Text])
    val max = mkSlotsString(maxTasks) 
    w.set(max.asInstanceOf[Array[Writable]])
    w
  }

  final def mkSlotsString(maxTasks: Int): Array[Text] = {
    val max = new Array[Text](maxTasks) 
    for(pos <- 0 until max.length) {
      max(pos) = new Text(nullString)
    }
    max
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
  protected[monitor] var s = defaultSlots(mt) // TODO: change to SlotStats { status [ taskattemptId | none | broken ], crash count by slot seq (int), max retries 
  
  def name(): String = n 

  def host(): String = h 

  def port(): Int = p

  def hostPort(): String = host + ":" + port
  
  def maxTasks(): Int = mt
 
  def slots(): Array[String] = s.toStrings

  @throws(classOf[IOException])
  override def write(out: DataOutput) {
    Text.writeString(out, name)
    Text.writeString(out, host)
    out.writeInt(port)
    out.writeInt(maxTasks)
    //q.write(out)
    s.write(out)
  }

  @throws(classOf[IOException])
  override def readFields(in: DataInput) {
    n = Text.readString(in)
    h = Text.readString(in)
    p = in.readInt
    mt = in.readInt
    //q = defaultQueue
    //q.readFields(in)
    s = defaultSlots(mt)
    s.readFields(in)
  }

  override def equals(o: Any): Boolean = o match {
    case that: GroomStats => that.isInstanceOf[GroomStats] &&
      that.n.equals(n) && that.h.equals(h) && (that.p == p) && 
      (that.mt == mt) /*&& that.q.toStrings.equals(q.toStrings)*/ &&
      that.s.toStrings.equals(s.toStrings) 
    case _ => false
  }
  
  override def hashCode(): Int = 
    41 * ( 
      //41 * ( 
        41 * ( 
          41 * (
            41 * (
              41 + n.toString.hashCode
            ) + h.toString.hashCode
          ) + p
        ) + mt
      //) + q.toStrings.hashCode
    ) + s.toStrings.hashCode
}

object SlotStats {

  val broken = "broken"

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

  //def slots(): Array[String] = ss.get.

  //def isBroken(seq: Int): Boolean = if(seq < slots.size) 
    //broken.equals(slots(seq)) else 
    //throw new RuntimeException("Invalid slot seq: "+seq+"!")
  
  @throws(classOf[IOException])
  override def write(out: DataOutput) {
   // TODO: 
  }

  @throws(classOf[IOException])
  override def readFields(in: DataInput) {
    // TODO:
  }

  override def equals(o: Any): Boolean = true // TODO: 

  override def hashCode(): Int = -1 // TODO:

}

final case class Inform(service: String, result: ProbeMessages) 
      extends ProbeMessages
