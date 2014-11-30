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
import org.apache.hadoop.io.ObjectWritable
import org.apache.hadoop.io.Writable
import org.apache.hama.HamaConfiguration
import org.apache.hama.master.Directive
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

  def apply(dest: String, data: Writable): Stats = {
    if(null == dest || dest.isEmpty)
      throw new IllegalArgumentException("Destination (tracker) is missing!")
    new Stats(dest, data)
  }

}


/**
 * Statistics data to be reported.
 * @param d is the destination to which this stats will be sent.
 * @param v is the stats collected.
 */
final class Stats(d: String, v: Writable) extends Writable with ProbeMessages {

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

}

/*
object TaskStats {

  def apply(tasks: Array[Task]): TaskStats = { 
    val stats = new TaskStats()
    val w = new ArrayWritable(classOf[Task])  
    w.set(tasks.asInstanceOf[Writable]) 
    stats.t = w
    stats
  }

}

final class TaskStats extends Writable with ProbeMessages {

  protected[monitor] var t: ArrayWritable = new ArrayWritable(classOf[])
 
  def tasks(): Array[] = t.get().asInstanceOf[]

  override def write(in: DataInput) {

  }

  override def readFields(out: DataOutput) { 

  }

}
*/

object GroomStats { // TODO: remove queue?

  def apply(name: String, host: String, port: Int, maxTasks: Int,
            queue: Array[String], slots: Array[String]): GroomStats = {
    val stats = new GroomStats
    stats.n = name
    stats.h = host
    stats.p = port
    stats.mt = maxTasks
    stats.q = toWritable(queue)
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
    stats.q = defaultQueue
    stats.s = defaultSlots(maxTasks)
    stats
  }

  def list(ds: Queue[Directive]): Array[String] = ds.map { d => d match {
    case null => nullString
     case _ => d.task.getId.toString
  }}.toArray

  def list(slots: Set[Slot]): Array[String] = slots.map { s => s.task match {
    case None => nullString
    case Some(t) => t.getId.toString
  }}.toArray

  final def toWritable(strings: Array[String]): ArrayWritable = {
    val w = new ArrayWritable(classOf[Text])
    w.set(strings.map { e => new Text(e) })
    w
  }

  final def defaultQueue(): ArrayWritable = {
    val w = new ArrayWritable(classOf[Text])
    w.set(Array[Text]().asInstanceOf[Array[Writable]])
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

  /* queue */
  protected[monitor] var q = defaultQueue

  /* slots */
  protected[monitor] var s = defaultSlots(mt)
  
  def name(): String = n 

  def host(): String = h 

  def port(): Int = p
  
  def maxTasks(): Int = mt
 
  def queue(): Array[String] = q.toStrings

  def slots(): Array[String] = s.toStrings

  @throws(classOf[IOException])
  override def write(out: DataOutput) {
    Text.writeString(out, name)
    Text.writeString(out, host)
    out.writeInt(port)
    out.writeInt(maxTasks)
    q.write(out)
    s.write(out)
  }

  @throws(classOf[IOException])
  override def readFields(in: DataInput) {
    n = Text.readString(in)
    h = Text.readString(in)
    p = in.readInt
    mt = in.readInt
    q = defaultQueue
    q.readFields(in)
    s = defaultSlots(mt)
    s.readFields(in)
  }

  override def equals(o: Any): Boolean = o match {
    case that: GroomStats => that.isInstanceOf[GroomStats] &&
      that.n.equals(n) && that.h.equals(h) && (that.p == p) && 
      (that.mt == mt) && that.q.toStrings.equals(q.toStrings) &&
      that.s.toStrings.equals(s.toStrings) 
    case _ => false
  }
  
  override def hashCode(): Int = 
    41 * ( 
      41 * ( 
        41 * ( 
          41 * (
            41 * (
              41 + n.toString.hashCode
            ) + h.toString.hashCode
          ) + p
        ) + mt
      ) + q.toStrings.hashCode
    ) + s.toStrings.hashCode
}

final case class Inform(service: String, result: ProbeMessages) 
      extends ProbeMessages
