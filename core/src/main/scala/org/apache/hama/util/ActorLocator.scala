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
package org.apache.hama.util

import org.apache.hama.HamaConfiguration
/*
import org.apache.hama.monitor.master.JvmStatsTracker
import org.apache.hama.monitor.master.GroomsTracker
import org.apache.hama.monitor.master.JobTasksTracker
import org.apache.hama.master.Scheduler
*/
import org.apache.hama.groom.GroomServer
import org.apache.hama.groom.TaskCounsellor
import org.apache.hama.ProxyInfo

/**
 * Magnet for modeling methods with similar purpose.
 */
trait ActorPathMagnet {

  /**
   * Define the return type of apply().
   */
  type Path

  /**
   * Serve for instance creation. 
   * @return Path type for general purpose.
   */
  def apply(): Path
}

/**
 * An uniform way to create a path with different purposes. 
 */
trait ActorLocator {

  /**
   * A method for creating path.
   * @param magnet that passes in different parameters.
   * @return Path type pointed to a particular actor. 
   */
  def locate(magnet: ActorPathMagnet): magnet.Path = magnet()

}

// TODO: remove? 
/*
final case class MasterLocator(info: ProxyInfo)
final case class JvmStatsTrackerLocator(conf: HamaConfiguration)
final case class GroomsTrackerLocator(conf: HamaConfiguration)
final case class JobTasksTrackerLocator(conf: HamaConfiguration)
final case class SchedulerLocator(conf: HamaConfiguration)
final case class ExecutorLocator(conf: HamaConfiguration)
*/
final case class TaskCounsellorLocator(conf: HamaConfiguration)

object ActorPathMagnet {

  import scala.language.implicitConversions 

/*
  implicit def locateMaster(locator: MasterLocator) = new ActorPathMagnet {
    type Path = String
    def apply(): Path = locator.info.getPath
  }

  // TODO: need refactor for retrieving path 
  implicit def locateSmr(locator: JvmStatsTrackerLocator) = 
      new ActorPathMagnet {
    type Path = String
    def apply(): Path = {
      new ProxyInfo.MasterBuilder("sysMetricsTracker", locator.conf).
                    createActorPath.
                    appendRootPath("bspmaster"). // TODO: from setting
                    appendChildPath("monitor").
                    appendChildPath("JvmStatsTracker").
                    build.
                    getPath
    }
  }

  implicit def locateGtt(locator: GroomsTrackerLocator) = 
      new ActorPathMagnet {
    type Path = String
    def apply(): Path = {
      new ProxyInfo.MasterBuilder("groomTasksTracker", locator.conf).
                    createActorPath.
                    appendRootPath("bspmaster"). // TODO: from setting
                    appendChildPath("monitor").
                    appendChildPath("GroomsTracker").
                    build.
                    getPath
    }
  }

  implicit def locateJtt(locator: JobTasksTrackerLocator) = 
      new ActorPathMagnet {
    type Path = String
    def apply(): Path = {
      new ProxyInfo.MasterBuilder("jobTasksTracker", locator.conf).
                    createActorPath.
                    appendRootPath("bspmaster"). // TODO: from setting
                    appendChildPath("monitor").
                    appendChildPath("JobTasksTracker").
                    build.
                    getPath
    }
  }
 
  implicit def locateSched(locator: SchedulerLocator) = 
      new ActorPathMagnet {
    type Path = String
    def apply(): Path = {
      new ProxyInfo.MasterBuilder("sched", locator.conf).
                    createActorPath.
                    appendRootPath("bspmaster").
                    appendChildPath("sched").
                    build.
                    getPath
    }
  }

  implicit def locateExecutor(locator: ExecutorLocator) = 
      new ActorPathMagnet {
    type Path = String
    def apply(): Path = {
       val groomName = locator.conf.get("bsp.groom.name", "groomServer")
       val slotSeq = locator.conf.getInt("bsp.child.slot.seq", 1)
       val executorName = groomName+"_executor_"+slotSeq
       new ProxyInfo.GroomBuilder(executorName, locator.conf). 
                     createActorPath.
                     appendRootPath(groomName).
                     appendChildPath("taskCounsellor").
                     appendChildPath(executorName).
                     build.
                     getPath
    }
  }
*/

  implicit def locateTaskCounsellor(locator: TaskCounsellorLocator) = 
      new ActorPathMagnet {
    type Path = String
    def apply(): Path = {
       val taskCounsellorName = TaskCounsellor.simpleName(locator.conf)
       val groomName = GroomServer.simpleName(locator.conf)
       new ProxyInfo.GroomBuilder(taskCounsellorName, locator.conf).
                    createActorPath.
                    appendChildPath(groomName).
                    appendChildPath(taskCounsellorName).
                    build.
                    getPath
    }
  }
}
