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
import org.apache.hama.LocalService
import org.apache.hama.HamaConfiguration
import org.apache.hama.conf.Setting
import org.apache.hama.monitor.Ganglion
import org.apache.hama.monitor.groom.TaskStatsCollector
import org.apache.hama.monitor.groom.GroomStatsCollector
import org.apache.hama.monitor.groom.JvmStatsCollector
import org.apache.hama.util.Curator

object Reporter {

   val defaultReporters = Seq(classOf[TaskStatsCollector].getName,
     classOf[GroomStatsCollector].getName, classOf[JvmStatsCollector].getName)

}

// TODO: receive stat message, dest to tracker, from collector and send to groom server.
class Reporter(setting: Setting, groom: ActorRef) 
      extends Ganglion with LocalService with Curator {

  import Reporter._

  override def initializeServices {
    val defaultClasses = setting.hama.get("reporter.default.plugin.classes",
                                          defaultReporters.mkString(","))
    load(defaultClasses).foreach( plugin => {
       LOG.debug("Default reporter to be instantiated: {}", plugin)
       getOrCreate(plugin.getName, plugin, new HamaConfiguration(setting.hama),
                   self)
    })
    LOG.debug("Finish loading default reporters ...")

    val classes = setting.hama.get("reporter.plugin.classes")
    val nonDefault = load(classes)
    nonDefault.foreach( plugin => {
       LOG.debug("Non default trakcer to be instantiated: {}", plugin)
       getOrCreate(plugin.getName, plugin, new HamaConfiguration(setting.hama),
                   self)
    })
    LOG.debug("Finish loading {} non default reporters ...", nonDefault.size)

/*
    initializeCurator(setting.hama)
    getOrCreate("tasksCollector", classOf[TasksCollector], setting.hama, self)
    getOrCreate("groomCollector", classOf[GroomCollector], setting.hama, self)
    getOrCreate("sysMetricsCollector", classOf[SysMetricsCollector], 
                setting.hama, self)
*/
  }

  def receive = unknown

}
