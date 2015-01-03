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
import org.apache.hama.Event
import org.apache.hama.EventListener
import org.apache.hama.SubscribeEvent
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

  // following functions will be implemented by wrapper.

  protected def listServices() 

  protected def servicesFound(services: Array[ActorRef]) 

  protected def findServiceBy(name: String)

  protected def serviceFound(service: ActorRef) 

  protected def subscribe(events: Event*) 
  
  protected def notified(result: Any) 

  // TODO: def unsubscribe(events: Event*) rmeove itself from event listener

  protected def publish(event: PublishEvent, msg: Any) 

  protected def funcs: Receive = {
    case ListService => listServices()
    case ServicesAvailable(services) => servicesFound(services) 
    case FindServiceBy(name) => findServiceBy(name)
    case ServiceAvailable(service) => serviceFound(service.getOrElse(null)) 
    case SubscribeTo(events) => subscribe(events)
    case Notification(result) => notified(result)
    case Publish(event, msg) => publish(event, msg)
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

  override def servicesFound(services: Array[ActorRef]) = 
    collector.servicesFound(services)

  override def findServiceBy(name: String) = reporter ! FindServiceBy(name)

  override def serviceFound(service: ActorRef) = collector.serviceFound(service)

  override def subscribe(events: Event*) = reporter ! SubscribeEvent(events:_*)

  override def notified(result: Any) = collector.notified(result) 

  override def publish(event: PublishEvent, msg: Any) = 
    reporter ! new PublishMessage {
      def event(): PublishEvent = ProbeEvent
      def msg(): Any = msg
    }

  def actions: Receive = {
    case StartTick(ms) => 
      cancellable = Option(tick(self, Ticker, delay = ms.millis)) 
    case CancelTick => cancellable.map { c => c.cancel } 
    /* forward request to the target groom service. */
    case GetMetrics(service, msg) => service forward msg 
    /* target groom service replies stats according to GetMetrics msg. */
    case stats: CollectedStats => collector.statsCollected(stats.data) 
    /** collector delegate for forwarding stats */
    case stats: Stats => reporter ! stats
  }

  override def receive = funcs orElse actions orElse tickMessage orElse unknown

}

class WrappedTracker(federator: ActorRef, tracker: Tracker) 
    extends WrappedProbe {

  override def preStart() = {
    tracker.wrapper = Option(self)
    tracker.initialize
  }

  override def listServices() = federator ! ListService

  override def servicesFound(services: Array[ActorRef]) = 
    tracker.servicesFound(services)

  override def findServiceBy(name: String) = federator ! FindServiceBy(name)

  override def serviceFound(service: ActorRef) = tracker.serviceFound(service)

  override def subscribe(events: Event*) = federator ! SubscribeEvent(events:_*)

  override def notified(result: Any) = tracker.notified(result) 

  override def publish(event: PublishEvent, msg: Any) = 
    federator ! new PublishMessage {
      def event(): PublishEvent = ProbeEvent
      def msg(): Any = msg
    }

  protected def actions: Receive = {
    case s: Stats => tracker.receive(s.data)
    case action: Any => tracker.askFor(action, sender)
  }

  override def receive = funcs orElse actions orElse unknown

}

trait Probe extends CommonLog { 

  protected var conf: HamaConfiguration = new HamaConfiguration

  protected[monitor] var wrapper: Option[ActorRef] = None

  protected[monitor] def listServices() = wrapper match { 
    case Some(found) => found ! ListService
    case None => throw new RuntimeException("Wrapper not found!")
  }

  protected[monitor] def servicesFound(services: Array[ActorRef]) { }

  protected[monitor] def findServiceBy(name: String) = wrapper match { 
    case Some(found) => found ! FindServiceBy(name)
    case None => throw new RuntimeException("Wrapper not found!")
  }

  protected[monitor] def serviceFound(service: ActorRef) { }

  protected[monitor] def subscribe(events: Event*) = wrapper match {
    case Some(found) => found ! SubscribeTo(events:_*)
    case None => throw new RuntimeException("Wrapper not found!")
  }

  /**
   * Notify when the event subscribed happens.
   * @param result being notified
   */
  protected[monitor] def notified(result: Any) { } 

  protected[monitor] def publish(event: PublishEvent, msg: Any) = wrapper match{
    case Some(found) => found ! Publish(event, msg)
    case None => throw new RuntimeException("Wrapper not found!")
  }

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
  protected[monitor] def setConfiguration(conf: HamaConfiguration) = 
    this.conf = conf

  /**
   * A reference - either WrappedCollector or WrappedTracker - that wraps this
   * probe object.
   * @return ActorRef for this probe.  
   */
  protected def getSelf(): ActorRef = wrapper match {
    case Some(found) => found 
    case None => throw new RuntimeException("Wrapper not found!") 
  }

  // def close()  TODO: close this probe?  

}

/**
 * Track specific stats from grooms and react if necessary.
 */
trait Tracker extends Probe {

  /**
   * Ask tracker to perform an action.
   * @param action that asks tracker to perform.
   * @param from which service sends out this action.
   */
  protected[monitor] def askFor(action: Any, from: ActorRef) { }

  /**
   * Receive data from colector.
   * @param data such as stats sent to this tracker.
   */
  protected[monitor] def receive(data: Writable) { }


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

  protected[monitor] def statsCollected(stats: Writable) { }

  /**
   * Obtain metrics exported by a specific service.
   */
  protected[monitor] def retrieve(service: ActorRef, msg: Any) = wrapper match {
    case Some(found) => found ! GetMetrics(service, msg) 
    case None => throw new RuntimeException("WrappedCollector not found!")
  }

  protected[monitor] def report(value: Writable) = wrapper match {
    case Some(found) => dest match {
     case null | "" => LOG.warning("The dest of stats is unknown!")
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
  protected[monitor] def request() { }

} 

// TODO: add quartz scheduler in the future
trait Ganglion {

  /**
   * Load probe instances found under monitor package.
   * @param conf is common configuration, either for master or groom. 
   * @param classes string of probe.
   * @return a seq of probe instances.
   */
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

trait Publisher extends EventListener { self: Agent => 

  /**
   * Receive a publish messsage. Publisher notifies participants who are
   * interested.
   */
  protected def publish: Receive = {
    case pub: PublishMessage => forward(pub.event)(Notification(pub.msg))
  }

}
