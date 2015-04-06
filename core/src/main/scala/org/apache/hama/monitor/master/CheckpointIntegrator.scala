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
package org.apache.hama.monitor.master

import akka.actor.ActorRef
import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.Path
import org.apache.hama.Agent
import org.apache.hama.Close
import org.apache.hama.HamaConfiguration
import org.apache.hama.bsp.BSPJobID
import org.apache.hama.conf.Setting
import org.apache.hama.util.Curator
import org.apache.hama.master.FindLatestCheckpoint
import org.apache.hama.master.JobFinishedEvent
import org.apache.hama.master.LatestCheckpoint
import org.apache.hama.monitor.Tracker
import org.apache.hama.monitor.Checkpointer
import scala.util.Failure
import scala.util.Success
import scala.util.Try

// TODO: find functions and create abstract class or trait required for spawn
/**
 * Superstep integrity checker.
 * @param setting is master setting.
 * @param jobId is the job id.
 * @param sueprstep is the current superstep value.
 * @param totalTasks tells how many bsp peers involve.
 */
class Checker(setting: Setting, jobId: String, superstep: Int, 
              totalTasks: Int) extends Agent with Curator {

  import Checkpointer._
  import CheckpointIntegrator._ 

  override def preStart() = try { verifyCheckpoint(totalTasks) match {
    case Success(successful) => if(successful) 
      create(verifiedPath(setting, jobId, superstep)) 
    case Failure(e) => LOG.warning("Fail verifying checkpoint due to {}", e) 
  }} finally { close }

  protected def verifyCheckpoint(totalTasks: Int): Try[Boolean] = try {
    initializeCurator(setting)
    val parent = dir(root(setting), jobId, superstep)
    val result = list(parent).count( znode => znode.split("\\"+dot) match {
      case ary: Array[String] if ary.length == 2 => if(ary(1).equals("ok"))
        true else false 
      case _ => throw new RuntimeException("Malformed znode found at "+znode)
    }) == totalTasks
    Success(result)
  } catch { case e: Exception => Failure(e) }

  override def receive = close orElse unknown

}

object CheckpointIntegrator {

  import Checkpointer._

  val superstepOf = "superstep-of-"

  def fullName(): String = classOf[CheckpointIntegrator].getName

  def verifiedPath(setting: Setting): String = 
    "%s/%s".format(root(setting), setting.get("bsp.checkpoint.verified.path", 
    "verified"))

  def verifiedPath(setting: Setting, jobId: String, superstep: Long): String = 
    "%s/%s/%s".format(verifiedPath(setting), jobId, superstep)

}

final class CheckpointIntegrator extends Tracker with Curator {

  import CheckpointIntegrator._

  private var children = Set.empty[ActorRef]

  override def initialize() {
    setSetting(Setting.master)
    subscribe(SuperstepIncrementEvent) 
    subscribe(JobFinishedEvent) 
  }

  override def notified(msg: Any) = msg match {
    case LatestSuperstep(jobId, n, totalTasks) => {
      children += spawn(superstepOf + n, classOf[Checker], // TODO: performance?
                        setting, jobId.toString, n, totalTasks)
    } 
    /**
     * Reset stats when a job finish execution. 
     */
    case jobId: BSPJobID => {
      children.foreach( child => child ! Close )
      children = Set.empty[ActorRef] 
    }
    case _ => LOG.warning("Unknown message {}!", msg)
  }

  override def askFor(action: Any, from: ActorRef) = action match {
    case FindLatestCheckpoint(jobId: BSPJobID) => {
      val pathToJobId = "%s/%s".format(verifiedPath(setting), 
                                       jobId.toString)
      val latest = latestSuperstep(pathToJobId) match {
        case Success(value) => value
        case Failure(cause) => {
          LOG.warning("Can't find the latest checkpoint because {}", cause)
          -1
        }
      }
      from ! LatestCheckpoint(jobId, latest)
    }
    case _ => LOG.warning("Unknown action {} from {}!", action, from.path.name)
  }

  private def latestSuperstep(pathToJobId: String): Try[Long] = Try(
    list(pathToJobId).map(_.toInt).max.toLong
  )
}

