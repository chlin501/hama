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
import org.apache.hadoop.io.Writable
import org.apache.hama.Agent
import org.apache.hama.HamaConfiguration
import org.apache.hama.Periodically
import org.apache.hama.Tick
import org.apache.hama.logging.CommonLog

sealed trait CollectorMessages
// from : plugin name
final case class Stats(from: String, data: Writable) extends CollectorMessages // extends writable

class WrappedCollector(reporter: ActorRef, collector: Collector) 
      extends Agent with Periodically {

  override def preStart() = collector.initialize

  override def ticker(tick: Tick) = {
    val stats = collector.collect()
    // TODO: send to reporter which in turn sends to groom then master and tracker
    //reporter ! Stats(collector.name, stats) 
  }

  override def receive = tickMessage orElse unknown

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
 * Collect specific groom stats.
 */
trait Collector extends Plugin {

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

