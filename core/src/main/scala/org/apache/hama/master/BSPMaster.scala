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
import org.apache.hama.bsp.BSPJobID
import org.apache.hama.conf.Setting
import org.apache.hama.groom.RequestTask
import org.apache.hama.groom.TaskFailure
import org.apache.hama.monitor.Inform
import org.apache.hama.monitor.ListService
import org.apache.hama.monitor.ServicesAvailable
import org.apache.hama.monitor.Stats
import org.apache.hama.util.Curator
import org.apache.zookeeper.CreateMode
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration
import scala.collection.immutable.IndexedSeq
import scala.collection.immutable.Vector

final case class CheckGroomsExist(jobId: BSPJobID, targetGrooms: Array[String])
final case class AllGroomsExist(jobId: BSPJobID)
final case class SomeGroomsNotExist(jobId: BSPJobID)

object Registrator {

  def apply(setting: Setting): Registrator = new Registrator(setting)

}

class Registrator(setting: Setting) extends Curator {

  initializeCurator(setting.hama)

  def mkPath(): String = {
    val sys = setting.info.getActorSystemName
    val host = setting.info.getHost
    val port = setting.info.getPort
    "/%s/%s_%s@%s:%s".format("masters", setting.name, sys, host, port)
  }

  def register() {
    val path = mkPath
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

// TODO: - refactor FSM (perhaps remove it)
//       - update internal stats to tracker
class BSPMaster(setting: Setting, registrator: Registrator) 
      extends LocalService with MembershipDirector { 

  import BSPMaster._

  // TODO: use strategy and shutdown if any exceptions are thrown

  override def initializeServices {
    registrator.register
    join(seedNodes)
    subscribe(self)
    val conf = setting.hama
    val federator = getOrCreate(Federator.simpleName(conf), classOf[Federator],
                                setting, self) 
    val receptionist = getOrCreate(Receptionist.simpleName(conf), 
                                   classOf[Receptionist], setting, federator) 
    getOrCreate(Scheduler.simpleName(conf), classOf[Scheduler], 
                conf, self, receptionist) 
    // TODO: change master state
  }

  override def stopServices = unsubscribe(self) 

  def seedNodes(): IndexedSeq[SystemInfo] = Vector(setting.info)

  override def groomLeave(name: String, host: String, port: Int) = 
    inform(GroomLeave(name, host, port), Federator.simpleName(setting.hama), 
           Scheduler.simpleName(setting.hama))

  protected def inform(message: Any, names: String*) = names.foreach( name => {
    LOG.debug("Going to inform service {} with result {}", names.mkString(","), 
             message)
    findServiceBy(name).map { service => service forward message }
  })

  protected def dispatch: Receive = {
    case Inform(service, result) => inform(result, service)
    case stats: Stats => inform(stats, Federator.simpleName(setting.hama))
    case ListService => listServices(sender)
  }

  protected def listServices(from: ActorRef) = 
    from ! ServicesAvailable(currentServices)

  protected def currentServices(): Array[String] = services.map { service => 
    service.path.name 
  }.toArray

  // TODO: move to membership director?
  protected def msgFromReceptionist: Receive = { 
    case CheckGroomsExist(jobId, targetGrooms) => {
      val uniqueGrooms = targetGrooms.map(_.trim).groupBy(k => k).keySet
      LOG.info("Client configures targets: {}", uniqueGrooms.mkString(","))
      val actual = uniqueGrooms.takeWhile( key => {
        val array = key.split(":")
        if(null == array || array.size != 2) {
          false
        } else {
          val host = array(0)
          val port = array(1) 
          val existsOrNot = groomsExist(host, port.toInt)
          if(!existsOrNot) LOG.debug("Requested groom with host {} port {} "+
                                     "not exist!", host, port)
          existsOrNot
       }
      }).size
      if(uniqueGrooms.size == actual) sender ! AllGroomsExist(jobId) 
      else sender ! SomeGroomsNotExist(jobId)
    }
  }

  protected def groomsExist(host: String, port: Int): Boolean = 
    grooms.exists( groom => 
      groom.path.address.host.equals(Option(host)) &&
      groom.path.address.port.equals(Option(port))
    )

  protected def msgFromSched: Receive = {
    case GetTargetRefs(infos) => {
      var matched = Array.empty[ActorRef] 
      var unmatched = Array.empty[String] 
      infos.foreach( info => grooms.find( groom => 
        groom.path.address.host.equals(Option(info.getHost)) &&
        groom.path.address.port.equals(Option(info.getPort))
      ) match {
        case Some(ref) => matched ++= Array(ref)
        case None => unmatched ++= Array(info.getHost+":"+info.getPort)
      })
      unmatched.length match {
        case 0 => sender ! TargetRefs(matched)
        case _ => sender ! SomeMatched(matched, unmatched)
      } 
    } 
  }

  protected def msgFromGroom: Receive = {
    case req: RequestTask => inform(req, Scheduler.simpleName(setting.hama))
  }

  protected def taskFailure: Receive = {
    case fault: TaskFailure => inform(fault, Scheduler.simpleName(setting.hama))
  }

  override def receive = taskFailure orElse msgFromGroom orElse msgFromSched orElse msgFromReceptionist orElse dispatch orElse membership orElse unknown
  
}
