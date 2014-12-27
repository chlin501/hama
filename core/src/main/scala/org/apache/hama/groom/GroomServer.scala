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
import akka.actor.ActorSystem
import akka.actor.Props
import akka.cluster.Member
import org.apache.hama.HamaConfiguration
import org.apache.hama.LocalService
import org.apache.hama.ProxyInfo
import org.apache.hama.RemoteService
import org.apache.hama.conf.Setting
import org.apache.hama.monitor.Stats
import org.apache.hama.monitor.ListService
import org.apache.hama.monitor.ServicesAvailable
import org.apache.hama.monitor.GetMetrics
import org.apache.hama.util.Curator
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

object MasterFinder {

  val pattern = """(\w+)_(\w+)@(\w+):(\d+)""".r

  def apply(setting: Setting): MasterFinder = new MasterFinder(setting)

}

class MasterFinder(setting: Setting) extends Curator {

  import MasterFinder._

  initializeCurator(setting.hama)

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

  def simpleName(conf: HamaConfiguration): String = conf.get(
    "groom.name",
    classOf[GroomServer].getSimpleName
  )

  def main(args: Array[String]) {
    val groom = Setting.groom
    val sys = ActorSystem(groom.info.getActorSystemName, groom.config)
    sys.actorOf(Props(groom.main, groom, MasterFinder(groom)), 
                      simpleName(groom.hama))
  }
}

// TODO: service may have metrics exportable (e.g. trait Exportable#getMetrics)
class GroomServer(setting: Setting, finder: MasterFinder) 
      extends LocalService with RemoteService with MembershipParticipant { 

  override def initializeServices {
    retry("lookupMaster", 10, lookupMaster)
    val reporter = getOrCreate(Reporter.simpleName(setting.hama),
                               classOf[Reporter], setting, self) 
    getOrCreate(TaskCounsellor.simpleName(setting.hama), 
                classOf[TaskCounsellor], setting, self, reporter)
  }

  override def stopServices = unsubscribe(self)

  override def masterFinder(): MasterFinder = finder 

  protected def report: Receive = {
    case stats: Stats => forwardToMaster(stats)
    case ListService => listServices(sender)
    case GetMetrics(serviceName, command) => findServiceBy(serviceName).map { 
      service => service forward command
    }
  }

  protected def forwardToMaster(msg: Any): Unit = master.map { m => 
    findProxyBy(m.getActorName).map { proxy => proxy forward msg }
  }

  protected def dispatch: Receive = {
    case req: RequestTask => forwardToMaster(req)
  }
  
  protected def listServices(from: ActorRef) = 
    from ! ServicesAvailable(currentServices)

  protected def currentServices(): Array[String] = services.map { service => 
    service.path.name 
  }.toArray

  protected def taskFailure: Receive = {
    case fault: TaskFailure => forwardToMaster(fault)  
  }

  override def receive = taskFailure orElse dispatch orElse report orElse actorReply orElse retryResult orElse membership orElse unknown
}
