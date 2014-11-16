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
import org.apache.hama.Agent

/*
protected calss CollectorWrapper(conf: HamaConfiguration, 
                                 collector: Collector) extends Actor {

  def initialize()

  // schedule to periodically call collect function.

}

protected class TrackerWrapper(conf: HamaConfiguration, tracker: Tracker) 
*/

trait Plugin extends Agent 

/**
 * Track specific stats from grooms.
 */
trait Tracker extends Plugin 

/**
 * Collect specific groom stats.
 */
trait Collector extends Plugin /* {

  def initialize 

  def collect(msg: Any): Any

} */

// TODO: add quartz scheduler in the future
trait Ganglion {

  protected def load(classes: String): Seq[Class[Plugin]] =
    if(null == classes || classes.isEmpty) Seq()
    else {
      val classNames = classes.split(",")
      classNames.map { className => {
        val clazz = Class.forName(className.trim)
        classOf[Plugin] isAssignableFrom clazz match {
          case true => Option(clazz.asInstanceOf[Class[Plugin]])
          case false => None
        }
      }}.toSeq.filter { e => !None.equals(e) }.map { e => e.getOrElse(null) }
    }

}

