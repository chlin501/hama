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
import org.apache.hama.monitor.Tracker
import org.apache.hama.monitor.master.GroomTasksTracker
import org.apache.hama.monitor.master.JobTasksTracker
import org.apache.hama.monitor.master.SysMetricsTracker

sealed trait FederatorMessages
final case object ListTrackers extends FederatorMessages
final case class TrackersAvailable(services: Seq[String]) 
      extends FederatorMessages

object Federator {

   def defaultTrackers(): Seq[String] = Seq(classOf[GroomTasksTracker].getName, 
     classOf[JobTasksTracker].getName, classOf[SysMetricsTracker].getName)

}


class Federator(setting: Setting) extends LocalService {

  import Federator._

  override def initializeServices {
    val defaultClasses = setting.hama.get("monitor.default.classes", 
                                          defaultTrackers.mkString(","))
    load(defaultClasses).foreach( option => option.map { tracker => {
       LOG.debug("Default trakcer to be instantiated: {}", tracker)
       getOrCreate(tracker.getName, tracker, 
                   new HamaConfiguration(setting.hama)) 
    }})
    LOG.debug("Finish loading default trackers ...")

    val classes = setting.hama.get("monitor.classes")
    val nonDefault = load(classes)
    nonDefault.foreach( option => option.map { tracker => {
       LOG.debug("Non default trakcer to be instantiated: {}", tracker)
       getOrCreate(tracker.getName, tracker, 
                   new HamaConfiguration(setting.hama)) 
    }})
    LOG.debug("Finish loading {} non default trackers ...", nonDefault.size)
  }

  protected def load(classes: String): Seq[Option[Class[Tracker]]] = 
    if(null == classes || classes.isEmpty) Seq()
    else {
      val classNames = classes.split(",")
      classNames.map { className => {
        LOG.debug("ClassName to be loaded is {}", className)
        val clazz = Class.forName(className.trim)
        classOf[Tracker].isAssignableFrom(clazz) match {
          case true => clazz match {
            case tracker: Class[Tracker] => Some(tracker)
            case _ => None
          }
          case false => None
        }
      }}.toSeq
    }

  protected def listTrackers: Receive = {
    case ListTrackers => replyTrackers(sender) 
  }

  protected def replyTrackers(from: ActorRef) = 
    from ! TrackersAvailable(currentTrackers())

  protected def currentTrackers(): Seq[String] = services.map { (service) => 
    service.path.name 
  }.toSeq

  override def receive = listTrackers orElse unknown

}
