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
import akka.actor.ActorSystem
import akka.actor.Address
import akka.actor.Props
import org.apache.hama.HamaConfiguration
import org.apache.hama.LocalService
import org.apache.hama.SystemInfo
import org.apache.hama.conf.Setting
import org.apache.hama.monitor.Inform
import org.apache.hama.monitor.Stats
import org.apache.hama.util.Curator
import org.apache.zookeeper.CreateMode
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration
import scala.collection.immutable.IndexedSeq
import scala.collection.immutable.Vector

object Registrator {

  def apply(setting: Setting): Registrator = new Registrator(setting)

}

class Registrator(setting: Setting) extends Curator {

  initializeCurator(setting.hama)

  def register() {
    val sys = setting.info.getActorSystemName
    val host = setting.info.getHost
    val port = setting.info.getPort
    val path = "/%s/%s_%s@%s:%s".format("masters", setting.name, sys, host,
                                        port)
    LOG.debug("Master znode will be registered at {}", path)
    create(path, CreateMode.EPHEMERAL)
  }
}

object BSPMaster {

  def simpleName(conf: HamaConfiguration): String = conf.get(
    "master.name",
    classOf[BSPMaster].getSimpleName
  )

  def main(args: Array[String]) {
    val master = Setting.master
    val sys = ActorSystem(master.info.getActorSystemName, master.config)
    sys.actorOf(Props(master.main, master, Registrator(master)), 
                simpleName(master.hama))
  }
 
}

// TODO: refactor FSM (perhaps remove it)
class BSPMaster(setting: Setting, registrator: Registrator) 
      extends LocalService with MembershipDirector { 

  import BSPMaster._

  override def initializeServices {
    registrator.register
    join(seedNodes)
    subscribe(self)
    val conf = setting.hama
    val receptionist = getOrCreate(Receptionist.simpleName(conf), 
                                   classOf[Receptionist], setting) 
    getOrCreate(Federator.simpleName(conf), classOf[Federator], setting, self) 
    getOrCreate(Scheduler.simpleName(conf), classOf[Scheduler], 
                conf, receptionist) 
    // TODO: change master state
  }

  override def stopServices = unsubscribe(self) 

  def seedNodes(): IndexedSeq[SystemInfo] = Vector(setting.info)

  override def groomLeave(name: String, host: String, port: Int) = 
    inform(GroomLeave(name, host, port), Federator.simpleName(setting.hama), 
           Scheduler.simpleName(setting.hama))

  protected def inform(message: Any, names: String*) = names.foreach( name => 
    findServiceBy(name).map { service => service ! message }
  )

  protected def dispatch: Receive = {
    case Inform(service, result) => inform(result, service)
    case stats: Stats => inform(stats, Federator.simpleName(setting.hama))
  }

  override def receive = dispatch orElse membership orElse unknown
  
}
