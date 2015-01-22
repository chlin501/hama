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
import org.apache.hama.Event
import org.apache.hama.HamaConfiguration
import org.apache.hama.LocalService
import org.apache.hama.EventListener
import org.apache.hama.RemoteService
import org.apache.hama.SystemInfo
import org.apache.hama.bsp.BSPJobID
import org.apache.hama.conf.Setting
import org.apache.hama.groom.RequestTask
import org.apache.hama.groom.TaskFailure
import org.apache.hama.monitor.FindServiceBy
import org.apache.hama.monitor.ListService
import org.apache.hama.monitor.ServiceAvailable
import org.apache.hama.monitor.ServicesAvailable
import org.apache.hama.monitor.Stats
import org.apache.hama.util.MasterDiscovery
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration
import scala.collection.immutable.IndexedSeq
import scala.collection.immutable.Vector

/**
 * An event signifies that a groom is offline.
 */
final case object GroomLeaveEvent extends Event
/**
 * An event signifies that stats data from a groom server arrives.
 */
final case object StatsArrivalEvent extends Event
/**
 * An event from a groom server requesting for assigning a new task.
 */
final case object RequestTaskEvent extends Event
/**
 * An event shows that a task running on the target groom server fails.
 */
final case object TaskFailureEvent extends Event

final case class CheckGroomsExist(jobId: BSPJobID, targetGrooms: Array[String])
final case class AllGroomsExist(jobId: BSPJobID)
final case class SomeGroomsNotExist(jobId: BSPJobID)

object BSPMaster {

  def simpleName(conf: HamaConfiguration): String = conf.get(
    "master.name",
    classOf[BSPMaster].getSimpleName
  )

  def main(args: Array[String]) {
    val master = Setting.master
    val sys = ActorSystem(master.info.getActorSystemName, master.config)
    sys.actorOf(Props(master.main, master), master.name)
  }
 
}

// TODO: - refactor FSM (perhaps remove it)
//       - renew master state when related funcs are finished
//       - update master state to tracker
class BSPMaster(setting: Setting) extends LocalService with RemoteService
                                                       with MasterDiscovery
                                                       with MembershipDirector 
                                                       with EventListener { 

  import BSPMaster._

  // TODO: use strategy and shutdown if any exceptions are thrown?

  override def setting(): Setting = setting

  override def initializeServices {
    register
    join(seedNodes)
    subscribe(self)
    val conf = setting.hama
    val federator = getOrCreate(Federator.simpleName(conf), classOf[Federator],
                                setting, self) 
    val receptionist = getOrCreate(Receptionist.simpleName(conf), 
                                   classOf[Receptionist], setting, federator) 
    getOrCreate(Scheduler.simpleName(conf), classOf[Scheduler], 
                conf, self, receptionist, federator) 
  }

  override def stopServices = {
    unsubscribe(self) 
    stopCurator
  }

  def seedNodes(): IndexedSeq[SystemInfo] = Vector(setting.info)

  override def groomLeave(name: String, host: String, port: Int) =  
    forward(GroomLeaveEvent)(GroomLeave(name, host, port))

  protected def dispatch: Receive = {
    /* Dispatch stats, from collector, to Federator */
    case stats: Stats => forward(StatsArrivalEvent)(stats) 
    case ListService => listServices(sender)
    case FindServiceBy(name) => sender ! ServiceAvailable(findServiceBy(name))
  }

  protected def listServices(from: ActorRef) = 
    from ! ServicesAvailable(services.toArray)

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
      // Note: infos may contain duplicate groom information, so don't change 
      //       matched and unmatched to Set, unless it's sure duplication can
      //       be preserved.
      var matched = Array.empty[ActorRef] 
      var nomatched = Array.empty[String] 
      infos.foreach( info => grooms.find( groom => 
        groom.path.address.host.equals(Option(info.getHost)) &&
        groom.path.address.port.equals(Option(info.getPort))
      ) match {
        case Some(ref) => matched ++= Array(ref)
        case None => nomatched ++= Array(info.getHost+":"+info.getPort)
      })
      // TODO: merge TargetRefs and SomeMatched into one e.g. sender ! TargetsFound(matched, nomatched)
      nomatched.isEmpty match { 
        case true => sender ! TargetRefs(matched)
        case false => sender ! SomeMatched(matched, nomatched)
      } 
      // case JobCompleteEvent TODO:  xxxx
    } 
    case FindGroomsToKillTasks(infos) => {
      var matched = Set.empty[ActorRef] 
      var nomatched = Set.empty[String] 
      infos.foreach( info => grooms.find( groom => 
        groom.path.address.host.equals(Option(info.getHost)) &&
        groom.path.address.port.equals(Option(info.getPort))
      ) match {
        case Some(ref) => matched += ref
        case None => nomatched += info.getHost+":"+info.getPort
      }) 
      sender ! GroomsFound(matched, nomatched)
    } 
    case FindGroomRef(host, port, newTask) => // TODO: find corresponded groom actor based on host port and return with newTask.
  }

  protected def msgFromGroom: Receive = {
    case req: RequestTask => forward(RequestTaskEvent)(req) 
    case fault: TaskFailure => forward(TaskFailureEvent)(fault)
  }

  //protected def msgFromClient: Receive = {
     //case AskForJobId => // TODO: generate new job id. sender ! NewJobID(id)
  //}

  override def receive = eventListenerManagement orElse msgFromGroom orElse msgFromSched orElse msgFromReceptionist orElse dispatch orElse membership orElse unknown
  
}
