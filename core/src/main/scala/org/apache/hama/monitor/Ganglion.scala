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
import akka.actor.Cancellable
import org.apache.hadoop.io.Writable
import org.apache.hama.Agent
import org.apache.hama.HamaConfiguration
import org.apache.hama.Periodically
import org.apache.hama.Tick
import org.apache.hama.Ticker
import org.apache.hama.master.GroomLeave
import org.apache.hama.logging.CommonLog
import scala.concurrent.duration.DurationLong
import scala.concurrent.duration.FiniteDuration

final case class StartTick(ms: Long)
final case object CancelTick

protected trait WrappedProbe extends Agent {

  protected def listServices() { }

  protected def servicesFound(services: Array[String]) { }

  protected def list: Receive = {
    case ListService => listServices()
    case ServicesAvailable(services) => servicesFound(services) 
  }

}

final class WrappedCollector(reporter: ActorRef, collector: Collector) 
      extends WrappedProbe with Periodically {

  import Collector._

  private var cancellable: Option[Cancellable] = None

  override def preStart() = {
    collector.wrapper = Option(self)
    collector.initialize
  }

  override def ticked(tick: Tick) = collector.request()

  override def listServices() = reporter ! ListService

  override def servicesFound(services: Array[String]) = 
    collector.servicesFound(services)

  def actions: Receive = {
    case StartTick(ms) => 
      cancellable = Option(tick(self, Ticker, delay = ms.millis)) 
    case CancelTick => cancellable.map { c => c.cancel } 
    case GetMetrics(service, command) => reporter ! GetMetrics(service, command)
    case stats: GroomStats => collector.statsFound(stats)
    /* collector sends to wrapper */
    case stats: Stats => reporter ! stats
  }

  override def receive = list orElse actions orElse tickMessage orElse unknown

}

class WrappedTracker(federator: ActorRef, tracker: Tracker) 
    extends WrappedProbe {

  override def preStart() = tracker.initialize

  override def listServices() = federator ! ListService

  override def servicesFound(services: Array[String]) = 
    tracker.servicesFound(services)

  def groomLeaves: Receive = {
    case GroomLeave(name, host, port) => tracker.groomLeaves(name, host, port)
  }

  def receiveStats: Receive = {
    case s: Stats => tracker.receive(s)
  }
  
  def askFor: Receive = {
    case msg: ProbeMessages => sender ! tracker.askFor(msg)
  }

  override def receive = askFor orElse receiveStats orElse list orElse groomLeaves orElse unknown

}

trait Probe extends CommonLog { 

  protected var conf: HamaConfiguration = new HamaConfiguration

  protected[monitor] var wrapper: Option[ActorRef] = None

  protected[monitor] def listServices() = wrapper match { 
    case Some(found) => found ! ListService
    case None => throw new RuntimeException("WrappedCollector not found!")
  }

  protected[monitor] def servicesFound(services: Array[String]) { }

  /**
   * The name of this probe.
   * @return String denotes the probe name.
   */
  def name(): String = getClass.getName

  /**
   * Initialize related functions before execution.
   */
  def initialize() { }

  /**
   * Common configuration of the groom server.
   * @return HamaConfiguration is the groom common configuration.
   */
  def configuration(): HamaConfiguration = conf

  /**
   * Common configuration of the groom server. 
   * @param conf is common configuration updating one held by this probe
   *             during instantiation. 
   */
  def setConfiguration(conf: HamaConfiguration) = this.conf = conf

  // TODO: close this probe?
  // def close() 

}

/**
 * Track specific stats from grooms and react if necessary.
 */
trait Tracker extends Probe {

  // TODO: list current master service
  // def notify() = ...local service... ?

  protected[monitor] def askFor(msg: ProbeMessages): ProbeMessages =
    EmptyProbeMessages

  protected[monitor] def receive(data: Writable) { }

  protected[monitor] def groomLeaves(name: String, host: String, port: Int) { }

}

object Collector {

  val EmptyStats = null.asInstanceOf[Writable]

}

/**
 * Collect specific groom stats data.
 */
trait Collector extends Probe {

  import Collector._

  /**
   * Start periodically collect stats data.
   */
  protected[monitor] def start(delay: Long = 3000) = wrapper match {  
    case Some(found) => found ! StartTick(delay)
    case None => throw new RuntimeException("WrappedCollector not found!")
  }

  /**
   * Cancel periodically collect stats data.
   */
  protected[monitor] def cancel() = wrapper match { 
    case Some(found) => found ! CancelTick
    case None => throw new RuntimeException("WrappedCollector not found!")
  }

  protected[monitor] def statsFound(stats: Writable) { }

  /**
   * Obtain metrics exported by a specific service.
   */
  protected[monitor] def retrieve(service: String, command: Any) = 
    wrapper match {
      case Some(found) => found ! GetMetrics(service, command) 
      case None => throw new RuntimeException("WrappedCollector not found!")
    }

  protected[monitor] def report(value: Writable) = wrapper match {
    case Some(found) => dest match {
     case null | "" =>
     case _ => found ! Stats(dest, value)
    }
    case None => throw new RuntimeException("WrappedCollector not found!")
  }

  /**
   * Destination, or tracker name, where stats will be send to. 
   */
  protected[monitor] def dest(): String

  /**
   * Periodically perform some execution.
   */
  protected[monitor] def request()


} 

// TODO: add quartz scheduler in the future
trait Ganglion {

  protected def load(conf: HamaConfiguration, classes: String): Seq[Probe] =
    if(null == classes || classes.isEmpty) Seq()
    else {
      val classNames = classes.split(",")
      classNames.map { className => {
        val clazz = Class.forName(className.trim)
        classOf[Probe] isAssignableFrom clazz match {
          case true => {
            val instance = clazz.asInstanceOf[Class[Probe]].newInstance
            instance.setConfiguration(conf)
            Option(instance)
          }
          case false => None
        }
      }}.toSeq.filter { e => !None.equals(e) }.map { e => e.getOrElse(null) }
    }

}

