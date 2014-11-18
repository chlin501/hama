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
import org.apache.hama.util.Utils._

sealed trait CollectorMessages
final case object ListService extends CollectorMessages
final case class ServicesAvailable(services: Array[String])
      extends CollectorMessages
final case class GetMetrics(service: String, command: Any)
      extends CollectorMessages
final case object GetGroomStats extends CollectorMessages
final case object GetTaskStats extends CollectorMessages

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
final class Stats(d: String, v: Writable) extends Writable
                                          with CollectorMessages {

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

object GroomStats {

  def apply(host: String, port: Int, maxTasks: Int): GroomStats = {
    val stats = new GroomStats
    stats.h = host
    stats.p = port
    stats.mt = maxTasks
    stats.q = defaultQueue
    stats.s = defaultSlots
    stats
  }

  def defaultQueue(): ArrayWritable = {
    val w = new ArrayWritable(classOf[Text])
    w.set(Array[Text]().asInstanceOf[Array[Writable]])
    w
  }

  def defaultSlots(): ArrayWritable = {
    val w = new ArrayWritable(classOf[Text])
    w.set(Array[Text]().asInstanceOf[Array[Writable]])
    w
  }
}

final class GroomStats extends Writable with CollectorMessages {

  import GroomStats._

  /* host */
  protected[monitor] var h: String = InetAddress.getLocalHost.getHostName

  /* port */
  protected[monitor] var p: Int = 50000 

  /* max tasks */
  protected[monitor] var mt: Int = 3

  /* queue */
  protected[monitor] var q = defaultQueue

  /* slots */
  protected[monitor] var s = defaultSlots
  
  def host(): String = h 

  def port(): Int = p
  
  def maxTasks(): Int = mt
 
  def queue(): Array[String] = q.toStrings

  def slots(): Array[String] = s.toStrings

  @throws(classOf[IOException])
  override def write(out: DataOutput) {
    Text.writeString(out, host)
    out.writeInt(port)
    out.writeInt(maxTasks)
    q.write(out)
    s.write(out)
  }

  @throws(classOf[IOException])
  override def readFields(in: DataInput) {
    h = Text.readString(in)
    p = in.readInt
    mt = in.readInt
    q = defaultQueue
    q.readFields(in)
    s = defaultSlots
    val grooms = new Array[Text](mt) // slots has max tasks constraint
    for(pos <- 0 until grooms.length) {
      grooms(pos) = new Text(nullString)
    }
    s.set(grooms.asInstanceOf[Array[Writable]])
    s.readFields(in)
  }
}

