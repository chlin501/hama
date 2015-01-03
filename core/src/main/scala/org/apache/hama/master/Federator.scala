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
import org.apache.hama.Event
import org.apache.hama.HamaConfiguration
import org.apache.hama.LocalService
import org.apache.hama.EventListener
import org.apache.hama.SubscribeEvent
import org.apache.hama.bsp.BSPJobID
import org.apache.hama.conf.Setting
import org.apache.hama.monitor.FindServiceBy
import org.apache.hama.monitor.Ganglion
import org.apache.hama.monitor.ListService
import org.apache.hama.monitor.Notification
import org.apache.hama.monitor.ProbeMessage
import org.apache.hama.monitor.PublishMessage
import org.apache.hama.monitor.Stats
import org.apache.hama.monitor.WrappedTracker
import org.apache.hama.monitor.master.GetMaxTasks
import org.apache.hama.monitor.master.GroomsTracker
import org.apache.hama.monitor.master.JobTasksTracker
import org.apache.hama.monitor.master.JvmStatsTracker
import org.apache.hama.monitor.master.TotalMaxTasks

final case class AskFor(recepiant: String, action: Any) extends ProbeMessage

sealed trait FederatorMessages
final case object ListTracker extends FederatorMessages

/**
 * This tells how many trackers are currently up.
 * @param trackers are trackers loaded.
 */
final case class TrackersAvailable(trackers: Array[String]) 
      extends FederatorMessages {
 
  override def toString(): String = 
    "TrackersAvailable("+trackers.mkString(",")+")"

}

object Federator {

  val defaultTrackers = Seq(GroomsTracker.fullName, 
    JobTasksTracker.fullName, JvmStatsTracker.fullName)

  def simpleName(conf: HamaConfiguration): String = conf.get(
    "master.federator.name", 
    classOf[Federator].getSimpleName
  )

}

class Federator(setting: Setting, master: ActorRef) 
      extends Ganglion with LocalService with EventListener {

  import Federator._

  /**
   * Constraints to be verified corresponds to a specific job. 
   */
  protected var validation = Set.empty[Validate]

  override def initializeServices {
    master ! SubscribeEvent(GroomLeaveEvent, StatsArrivalEvent)
    LOG.debug("Listening to groom leave and stats arrival events!")

    val defaultClasses = defaultTrackers.mkString(",")
    load(setting.hama, defaultClasses).foreach( probe => { 
       LOG.debug("Default trakcer to be instantiated: {}", probe.name)
       getOrCreate(probe.name, classOf[WrappedTracker], self, probe) 
    })
    LOG.debug("Finish loading default trackers ...")

    val classes = setting.hama.get("federator.probe.classes")
    val nonDefault = load(setting.hama, classes)
    nonDefault.foreach( probe => {
       LOG.debug("Non default trakcer to be instantiated: {}", probe.name)
       getOrCreate(probe.name, classOf[WrappedTracker], self, probe) 
    })
    LOG.debug("Finish loading {} non default trackers ...", nonDefault.size)
  }

  protected def listTracker: Receive = {
    case ListTracker => listTrackers(sender) 
  }

  protected def listTrackers(from: ActorRef) = 
    from ! TrackersAvailable(currentTrackers)

  protected def currentTrackers(): Array[String] = services.map { tracker => 
    tracker.path.name 
  }.toArray

  protected def dispatch: Receive = {
    /**
     * Ask tracker executing a specific action.
     */
    case AskFor(recepiant: String, action: Any) => askFor(recepiant, action)
    /**
     * Stats comes from collector, destined to a particular tracker.
     */
    case stats: Stats => findServiceBy(stats.dest).map { tracker => 
       tracker forward stats
    }
    /**
     * List master services currently available.
     */
    case ListService => master forward ListService
    case req: FindServiceBy => master forward req 
  }

  protected def askFor(recepiant: String, action: Any) =
    findServiceBy(recepiant).map { tracker => tracker forward action }

  /**
   * Subscribe GroomLeaveEvent to BSPMaster will get notified when a groom
   * leaves.
   */
  protected def events: Receive = {
    case groomLeave: GroomLeave => 
      notify(GroomLeaveEvent)(Notification(groomLeave))
  } 

  // TODO: move validate related functions to trait JobValidator then extends it
  protected def validate: Receive = { 
    case constraint: Validate => {
      cache(constraint)
      constraint.actions.keySet.foreach( action => action match { 
        case CheckMaxTasksAllowed => askFor(classOf[GroomsTracker].getName, 
          GetMaxTasks(constraint.jobId.toString))
        case IfTargetGroomsExist => {
          // TODO: targetGrooms string is created by client (check with master)
          val targetGrooms = constraint.jobConf.getStrings("bsp.target.grooms") 
          targetGrooms match {
            case null | Array() => {
              LOG.debug("Target grooms are not configured for {}.", 
                       constraint.jobId)
              update(constraint, 
                     constraint.actions.updated(IfTargetGroomsExist, Valid))
              postCheckFor(constraint.jobId)
            }
            case _ => master ! CheckGroomsExist(constraint.jobId, targetGrooms)
          }
        }
        case _ => LOG.warning("Unknown validation action: {}", action)
      })
    }
    case TotalMaxTasks(jobId, available) => 
      validateTotalMaxTasks(jobId, available) 
    case AllGroomsExist(jobId) => validateAllGroomsExist(jobId)
    case SomeGroomsNotExist(jobId) => validateSomeGroomsNotExist(jobId)
  }

  protected def validateSomeGroomsNotExist(jobId: BSPJobID) {
    updateBy(jobId)(IfTargetGroomsExist, 
                    Invalid("Some target grooms doesn't exists!")) 
    postCheckFor(jobId)
  }

  protected def validateAllGroomsExist(jobId: BSPJobID) {
    updateBy(jobId)(IfTargetGroomsExist, Valid)
    postCheckFor(jobId)
  }

  // TODO: findValidateBy too many times
  protected def validateTotalMaxTasks(jobId: String, available: Int) {
    val id = BSPJobID.forName(jobId) 
    val validate = findValidateBy(id)
    val requested = validate.jobConf.getInt("bsp.peers.num", 1)
    LOG.debug("Client requests {} tasks and the system has {}", requested, 
             available)
    (available >= requested) match {
      case true => updateBy(id)(CheckMaxTasksAllowed, Valid)
      case false => updateBy(id)(CheckMaxTasksAllowed, 
                    Invalid("Requested tasks ("+requested+") is larger "+
                            "than total tasks allowed ("+available+")"))
    }
    postCheckFor(id)
  }

  /**
   * Post check for a particular job id if all actions are verified; if true,
   * notify receptionist for further reaction; otherwise do nothing and wait 
   * for other validation result.
   * @param jobId denotes a job id associated with a validate object.
   */
  protected def postCheckFor(jobId: BSPJobID) = areAllVerified(jobId).map { v=> 
    v.receptionist ! v.validated
    validation -= v
  }

  /** 
   * If all constraints are verified, either Valid or Invalid, send back to 
   * receptionist for further actions, such reject or put to wait queue.
   * Otherwise do nothing and wait for other validation result.
   */
  protected def areAllVerified(jobId: BSPJobID): Option[Validate] = {
    val validate = findValidateBy(jobId)
    (0 == validate.actions.filter( e => e._2.equals(NotVerified)).size) match {
      case true => Option(validate)
      case false => None
    }
  }

  /**
   * Update cooresponded validate object through job id provided with key and 
   * value.
   * @param jobId is the id for the job to be verified.
   * @param key of the action indicating which property to be verified.
   * @param value is the validation result 
   */
  protected def updateBy(jobId: BSPJobID)(key: Any, value: Validation) {
    val validate = findValidateBy(jobId)
    update(validate, validate.actions.updated(key, value))
  }

  protected def findValidateBy(jobId: BSPJobID): Validate = validation.find(v =>
    v.jobId.equals(jobId)
  ) match {
    case Some(found) => found
    case None => throw new RuntimeException("Validate not found for "+
                                            jobId.toString+"!")
  }

  /**
   * Cache validate object.
   * @param v is the validation object.
   */
  protected def cache(v: Validate) = validation ++= Set(v)

  /**
   * Update validation set with corresponded validation result.
   * @param old validate object, containing job id, job conf, and client ref.
   * @param updated is a map containing validation action and result.
   */
  protected def update(old: Validate, updated: Map[Any, Validation])
                      (implicit jobId: String = old.jobId.toString) = 
    validation = validation.map { e => e.jobId.toString match {
      case `jobId` => Validate(old.jobId, old.jobConf, old.client,
                               old.receptionist, updated)
      case _ => e
    }}

  protected def publish: Receive = {
    case pub: PublishMessage => forward(pub.event)(Notification(pub.msg))
  }

  override def receive = eventListenerManagement orElse publish orElse validate orElse events orElse dispatch orElse listTracker orElse unknown

}
