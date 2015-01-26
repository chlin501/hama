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
import java.text.SimpleDateFormat
import java.util.Date
import org.apache.hadoop.fs.Path
import org.apache.hama.Event
import org.apache.hama.HamaConfiguration
import org.apache.hama.LocalService
import org.apache.hama.EventListener
import org.apache.hama.RemoteService
import org.apache.hama.SystemInfo
import org.apache.hama.bsp.BSPJobID
import org.apache.hama.client.Request
import org.apache.hama.client.Response
import org.apache.hama.conf.Setting
import org.apache.hama.fs.Operation
import org.apache.hama.groom.RequestTask
import org.apache.hama.groom.TaskFailure
import org.apache.hama.monitor.FindServiceBy
import org.apache.hama.monitor.ListService
import org.apache.hama.monitor.ServiceAvailable
import org.apache.hama.monitor.ServicesAvailable
import org.apache.hama.monitor.Stats
import org.apache.hama.monitor.master.ClientMaxTasksAllowed
import org.apache.hama.monitor.master.ClientTasksAllowed
import org.apache.hama.monitor.master.GroomsTracker
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
/**
 * An event when a client submit its job to master.
 */
final case object JobSubmitEvent extends Event

final case class FileSystemCleaned(systemDir: Path)
final case class CheckGroomsExist(jobId: BSPJobID, targetGrooms: Array[String])
// TODO: merge AllGroomsExist with SomeGroomsNotExist to e.g. GroomsExistsResult(found, notexist)
final case class AllGroomsExist(jobId: BSPJobID)
final case class SomeGroomsNotExist(jobId: BSPJobID)
final case class ResponseForClient(client: ActorRef, sysDir: Path, 
                                   maxTasks: Int)

object BSPMaster {

  private def newIdentifier(): String = {
    return new SimpleDateFormat("yyyyMMddHHmm").format(new Date())
  }

  def simpleName(conf: HamaConfiguration): String = conf.get(
    "master.name",
    classOf[BSPMaster].getSimpleName
  )

  def main(args: Array[String]) {
    val master = Setting.master
    val sys = ActorSystem(master.info.getActorSystemName, master.config)
    sys.actorOf(Props(master.main, master, newIdentifier), master.name)
  }
 
}

object FileSystemCleaner {

  def simpleName(conf: HamaConfiguration): String = conf.get(
    "master.fs.cleaner", classOf[FileSystemCleaner].getSimpleName)

}

protected[master] class FileSystemCleaner(setting: Setting, master: ActorRef) 
      extends LocalService {

  import Operation._

  protected val operation = Operation.get(setting.hama)

  override def initializeServices = if(setting.hama.getBoolean(
    "master.fs.cleaner.start", true)) retry("clean", 10, clean)

  protected def systemDir(): Path = new Path(operation.makeQualified(new Path(
    setting.hama.get("bsp.system.dir", "/tmp/hadoop/bsp/system"))))

  protected def clean(): Boolean = 
    operation.remove(systemDir) && operation.mkdirs(systemDir, sysDirPermission)

  override protected def retryCompleted(name: String, ret: Any) = name match {
    case "clean" => {
      master ! FileSystemCleaned(systemDir)
      LOG.info("File system is cleanup, stop {}!", name)
      stop
    }
    case _ => { LOG.error("Unexpected result {} when cleanup!", ret); shutdown }
  }

  override protected def retryFailed(name: String, cause: Throwable) = {
    LOG.error("Shutdown system due to error {} when trying {}!", cause, name)
    shutdown
  }

  override def receive = unknown

}

// TODO: - refactor FSM (perhaps remove it)
//       - renew master state when related funcs are finished
//       - update master state to tracker
class BSPMaster(setting: Setting, identifier: String) extends LocalService 
                                                      with RemoteService
                                                      with MasterDiscovery
                                                      with MembershipDirector 
                                                      with EventListener { 

  import BSPMaster._

  /* value for the next job id. */
  protected var nextJobId: Int = 1

  protected var systemDir: Option[Path] = None

  // TODO: use strategy and shutdown if any exceptions are thrown?

  protected var clientRequest = Map.empty[BSPJobID, ResponseForClient]

  override def setting(): Setting = setting

  override def initializeServices {
    cleaner
    register
    join(seedNodes)
    subscribe(self)
    val conf = setting.hama
    val federator = getOrCreate(Federator.simpleName(conf), classOf[Federator],
                                setting, self) 
    val receptionist = getOrCreate(Receptionist.simpleName(conf), 
                                   classOf[Receptionist], setting, self, 
                                   federator) 
    getOrCreate(Scheduler.simpleName(conf), classOf[Scheduler], 
                setting, self, receptionist, federator) 
  }

  protected def cleaner() = spawn(FileSystemCleaner.simpleName(setting.hama), 
    classOf[FileSystemCleaner], setting, self)

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
      // case JobCompleteEvent TODO:  
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

  protected def processClientRequest(from: ActorRef) {
    val jobId = newJobId
    clientRequest += (jobId -> ResponseForClient(from,
      systemDir.getOrElse(null), -1))
    findServiceBy(Federator.simpleName(setting.hama)) match {
      case Some(federator)  => federator ! AskFor(GroomsTracker.fullName, 
        ClientMaxTasksAllowed(jobId)) 
      case None =>
    }
    LOG.info("Client {} obtains job id {}, and system dir {}", from.path.name, 
             jobId, systemDir)
  }

  protected def clientRelatedMsg: Receive = {
    case Request => processClientRequest(sender)
    case ClientTasksAllowed(jobId, maxTasks) => clientRequest.get(jobId) match {
      case Some(response) => { 
        response.client ! Response(jobId, response.sysDir, maxTasks) 
        clientRequest -= jobId 
      }
      case None => LOG.warning("No client requests max tasks with jod id {}!",
                               jobId)
    }
    case submit: Submit => forward(JobSubmitEvent)(submit)
  }

  protected def newJobId(): BSPJobID = {
    val id = nextJobId
    nextJobId = id + 1
    new BSPJobID(identifier, id)
  }

  override def receive = cleanfs  

  protected def cleanfs: Receive = {
    case FileSystemCleaned(sysDir) => {
      systemDir = Option(sysDir)
      LOG.info("File system {} is cleaned!", sysDir)
      context.become(opened) 
    }
  }

  protected def opened: Receive = eventListenerManagement orElse clientRelatedMsg orElse msgFromGroom orElse msgFromSched orElse msgFromReceptionist orElse dispatch orElse membership orElse unknown
  
}

