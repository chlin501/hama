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

import akka.actor.ActorSystem
import akka.actor.Props
import akka.cluster.Member
import org.apache.hama.LocalService
import org.apache.hama.ProxyInfo
import org.apache.hama.RemoteService
import org.apache.hama.conf.Setting
import org.apache.hama.util.Curator
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

object MasterFinder {

  def apply(setting: Setting): MasterFinder = new MasterFinder(setting)

}

class MasterFinder(setting: Setting) extends Curator {

  initializeCurator(setting.hama)

  protected val pattern = """(\w+)_(\w+)@(\w+):(\d+)""".r

  def masters(): Array[ProxyInfo] = list("/masters").map { child => {
    LOG.debug("Master znode found is {}", child)
    val conf = setting.hama
    val ary = pattern.findAllMatchIn(child).map { m =>
      val name = m.group(1)
      conf.set("master.name", name)
      val sys = m.group(2)
      conf.set("master.actor-system.name", sys)
      val host = m.group(3)
      conf.set("master.host", host)
      val port = m.group(4).toInt
      conf.setInt("master.port", port)
      new ProxyInfo.MasterBuilder(name, conf).build
    }.toArray
    ary(0)
  }}.toArray

}

object GroomServer {

  def main(args: Array[String]) {
    val groom = Setting.groom
    val sys = ActorSystem(groom.info.getActorSystemName, groom.config)
    sys.actorOf(Props(classOf[GroomServer], groom, MasterFinder(groom)), 
                      groom.name)
  }
}

class GroomServer(setting: Setting, finder: MasterFinder) 
      extends LocalService with RemoteService with MembershipParticipant { 

  // TODO: pass sys info to task manager

  override def initializeServices {
    join
    subscribe(self)
    val monitor = getOrCreate("monitor", classOf[Reporter], setting.hama) 
    getOrCreate("taskManager", classOf[TaskManager], setting.hama, monitor) 
  }

  override def stopServices = unsubscribe(self)

  override def masterFinder(): MasterFinder = finder 

  override def receive = membership orElse unknown
}
