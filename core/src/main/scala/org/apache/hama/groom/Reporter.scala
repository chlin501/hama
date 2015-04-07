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
import org.apache.hama.Event
import org.apache.hama.HamaConfiguration
import org.apache.hama.LocalService
import org.apache.hama.conf.Setting
import org.apache.hama.monitor.CollectedStats
import org.apache.hama.monitor.FindServiceBy
import org.apache.hama.monitor.Ganglion
import org.apache.hama.monitor.ListService
import org.apache.hama.monitor.Notification
import org.apache.hama.monitor.Publisher
import org.apache.hama.monitor.PublishEvent
import org.apache.hama.monitor.PublishMessage
import org.apache.hama.monitor.Stats
import org.apache.hama.monitor.WrappedCollector
import org.apache.hama.monitor.groom.TaskStatsCollector
import org.apache.hama.monitor.groom.GroomStatsCollector
import org.apache.hama.monitor.groom.JvmStatsCollector

final case object TaskReportEvent extends PublishEvent 

final case object ListCollector
final case class CollectorsAvailable(names: Array[String]) {

  override def toString(): String = 
    "CollectorsAvailable(" +names.mkString(",")+ ")"

}

object Reporter {

  val defaultCollectors = Seq(TaskStatsCollector.fullName,
    GroomStatsCollector.fullName, JvmStatsCollector.fullName)

  def simpleName(setting: Setting): String = setting.get(
    "groom.reporter.name" ,
    classOf[Reporter].getSimpleName
  )

}

// TODO: periodically reload probes from reporter.probe.classes?
class Reporter(setting: Setting, groom: ActorRef) 
      extends Ganglion with LocalService with Publisher {

  import Reporter._

  override def initializeServices {
    val defaultClasses = defaultCollectors.mkString(",")
    load(setting, defaultClasses).foreach( probe => {
       LOG.debug("Default reporter to be instantiated: {}", probe.name)
       getOrCreate(probe.name, classOf[WrappedCollector], self, probe)
    })
    LOG.debug("Finish loading default reporters ...")

    val classes = setting.hama.get("reporter.probe.classes")
    val nonDefault = load(setting, classes)
    nonDefault.foreach( probe => {
       LOG.debug("Non default trakcer to be instantiated: {}", probe.name)
       getOrCreate(probe.name, classOf[WrappedCollector], self, probe)
    })
    LOG.debug("Finish loading {} non default reporters ...", nonDefault.size)
  }

  protected def currentCollectors(): Array[String] = services.map { service =>
    service.path.name
  }.toArray

  /**
   * Report functions 
   * - forward writable Stats to master.
   * - list groom server services available.
   */
  protected def report: Receive = {
    case stats: Stats => groom forward stats  
    case ListService => groom forward ListService 
    case req: FindServiceBy => groom forward req 
    case ListCollector => sender ! CollectorsAvailable(currentCollectors)
  }

  /**
   * Receive a publish messsage. Reporter notifies to participant who is 
   * interested.
  protected def publish: Receive = {
    case pub: PublishMessage => forward(pub.event)(Notification(pub.msg))
  }
   */

  override def receive = eventListenerManagement orElse publish orElse report orElse unknown

}
