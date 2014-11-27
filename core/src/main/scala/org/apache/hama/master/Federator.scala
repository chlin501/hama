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
package org.apache.hama.master

import akka.actor.ActorRef
import org.apache.hama.Agent
import org.apache.hama.HamaConfiguration
import org.apache.hama.LocalService
import org.apache.hama.conf.Setting
import org.apache.hama.monitor.Ganglion
import org.apache.hama.monitor.Inform
import org.apache.hama.monitor.ProbeMessages
import org.apache.hama.monitor.Stats
import org.apache.hama.monitor.WrappedTracker
import org.apache.hama.monitor.master.GroomsTracker
import org.apache.hama.monitor.master.JobTasksTracker
import org.apache.hama.monitor.master.JvmStatsTracker

final case class AskFor(recepiant: String, action: ProbeMessages) 
      extends ProbeMessages

sealed trait FederatorMessages
final case object ListTrackers extends FederatorMessages
/**
 * This tells how many trackers are currently up.
 * @param services are trackers loaded.
 */
final case class TrackersAvailable(services: Seq[String]) 
      extends FederatorMessages

object Federator {

  val defaultTrackers = Seq(classOf[GroomsTracker].getName, 
    classOf[JobTasksTracker].getName, classOf[JvmStatsTracker].getName)

  def simpleName(conf: HamaConfiguration): String = conf.get(
    "master.federator.name", 
    classOf[Federator].getSimpleName
  )

}

class Federator(setting: Setting, master: ActorRef) 
      extends Ganglion with LocalService {

  import Federator._

  override def initializeServices {
    val defaultClasses = defaultTrackers.mkString(",")
    load(setting.hama, defaultClasses).foreach( probe => { 
       LOG.debug("Default trakcer to be instantiated: {}", probe.name)
       getOrCreate(probe.name, classOf[WrappedTracker], self, probe) 
    })
    LOG.debug("Finish loading default trackers ...")

    val classes = setting.hama.get("federator.probe.classes")
    val nonDefault = load(setting.hama, classes)
    nonDefault.foreach( probe => {
       LOG.debug("Non default trakcer to be instantiated: {}", probe.name)
       getOrCreate(probe.name, classOf[WrappedTracker], self, probe) 
    })
    LOG.debug("Finish loading {} non default trackers ...", nonDefault.size)
  }

  protected def listTrackers: Receive = {
    case ListTrackers => replyTrackers(sender) 
  }

  protected def replyTrackers(from: ActorRef) = 
    from ! TrackersAvailable(currentTrackers())

  protected def currentTrackers(): Seq[String] = services.map { (tracker) => 
    tracker.path.name 
  }.toSeq

  protected def dispatch: Receive = {
    case AskFor(recepiant: String, action: ProbeMessages) => 
      findServiceBy(recepiant).map { tracker => tracker forward action }
    case stats: Stats => findServiceBy(stats.dest).map { tracker => 
       tracker forward stats
    }
  }

  def groomLeaveEvent: Receive = {
    case event: GroomLeave => services.foreach( tracker => tracker ! event)
  } 

  def inform: Receive = {
    case Inform(service, result) => master ! Inform(service, result)
  }

  override def receive = inform orElse groomLeaveEvent orElse dispatch orElse listTrackers orElse unknown

}
