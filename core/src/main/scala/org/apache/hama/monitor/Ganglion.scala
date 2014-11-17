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

import akka.actor.Actor
import akka.actor.ActorRef
import java.io.DataInput
import java.io.DataOutput
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.ObjectWritable
import org.apache.hadoop.io.Writable
import org.apache.hama.Agent
import org.apache.hama.HamaConfiguration
import org.apache.hama.Periodically
import org.apache.hama.Tick
import org.apache.hama.logging.CommonLog

sealed trait CollectorMessages
final case object ListService extends CollectorMessages
final case class ServicesAvailable(services: Array[String]) 
      extends CollectorMessages
final case class GetMetrics(service: String/*, command: Any*/) extends CollectorMessages

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

  var tracker: Text = new Text(d)
  var value: Writable = v 

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

class WrappedCollector(reporter: ActorRef, collector: Collector) 
      extends Agent with Periodically {

  override def preStart() = {
    collector.wrapper = Option(self)
    collector.initialize
  }

  override def ticker(tick: Tick) = collector.collect() match {
    case null =>
    case data@_ => reporter ! Stats(collector.dest, data) 
  }

  def services: Receive = {
    case ListService => reporter ! ListService
    case ServicesAvailable(services) => collector.servicesFound(services)
    case GetMetrics(service) => reporter ! GetMetrics(service)
  }

  override def receive = services orElse tickMessage orElse unknown

}

class WrappedTracker(federator: ActorRef, tracker: Tracker) extends Agent {

  override def preStart() = tracker.initialize
  
  // TODO: see Tracker TODO

  override def receive = unknown

}

trait Plugin extends CommonLog { 

  protected var conf: HamaConfiguration = new HamaConfiguration

  def name(): String = getClass.getName

  def initialize 

  def configuration(): HamaConfiguration = conf

  def setConfiguration(conf: HamaConfiguration) = this.conf = conf

}

/**
 * Track specific stats from grooms and react if necessary.
 */
trait Tracker extends Plugin {

  // TODO: list current master service
  //       ability to receive message from groom (master on behalf of groom)
  //       react function

  // def whenReceived(msg: Writable) {
    // need ability to know services available and send to target  
     //trigger to notify target(where target is mater service actor ref name)
  //}

}

/**
 * Collect specific groom stats data.
 */
trait Collector extends Plugin {

// TODO: find services available
//       obtain metrics or service execut provided collector func and return metrics (safety)

  protected[monitor] var wrapper: Option[ActorRef] = None

  def listServices() = wrapper match { 
    case Some(found) => found ! ListService
    case None => throw new RuntimeException("WrappedCollector not found!")
  }

  def servicesFound(services: Array[String]) { }

  /**
   * Obtain metrics exported by a specific service.
   */
  def getMetrics(service: String) = wrapper match {
    case Some(found) => found ! GetMetrics(service) 
    case None => throw new RuntimeException("WrappedCollector not found!")
  }

  /**
   * Destination, or tracker name, where stats will be send to. 
   */
  def dest(): String

  /**
   * Collect stats function.
   */
  def collect(): Writable 

} 

// TODO: add quartz scheduler in the future
trait Ganglion {

  protected def load(conf: HamaConfiguration, classes: String): Seq[Plugin] =
    if(null == classes || classes.isEmpty) Seq()
    else {
      val classNames = classes.split(",")
      classNames.map { className => {
        val clazz = Class.forName(className.trim)
        classOf[Plugin] isAssignableFrom clazz match {
          case true => {
            val instance = clazz.asInstanceOf[Class[Plugin]].newInstance
            instance.setConfiguration(conf)
            Option(instance)
          }
          case false => None
        }
      }}.toSeq.filter { e => !None.equals(e) }.map { e => e.getOrElse(null) }
    }

}

