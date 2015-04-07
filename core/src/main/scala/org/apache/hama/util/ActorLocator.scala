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

import org.apache.hama.conf.Setting
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

final case class TaskCounsellorLocator(setting: Setting)

object ActorPathMagnet {

  import scala.language.implicitConversions 

  implicit def locateTaskCounsellor(locator: TaskCounsellorLocator) = 
      new ActorPathMagnet {
    type Path = String
    def apply(): Path = {
       val taskCounsellorName = TaskCounsellor.simpleName(locator.setting)
       val groomName = GroomServer.simpleName(locator.setting)
       new ProxyInfo.GroomBuilder(taskCounsellorName, locator.setting.hama).
                    createActorPath.
                    appendRootPath(groomName). 
                    appendChildPath(taskCounsellorName).
                    build.
                    getPath
    }
  }
}
