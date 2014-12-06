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
import java.io.DataInputStream
import java.io.DataInput
import java.io.DataOutput
import java.io.IOException
import java.util.Arrays
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.WritableUtils
import org.apache.hama.HamaConfiguration
import org.apache.hama.LocalService
import org.apache.hama.bsp.BSPJobID
import org.apache.hama.bsp.v2.Job
import org.apache.hama.conf.Setting
import org.apache.hama.fs.Operation
import org.apache.hama.io.PartitionedSplit
import scala.collection.immutable.Queue
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration
import scala.util.Failure
import scala.util.Success
import scala.util.Try

sealed trait Validation
final case object NotVerified extends Validation
final case object Valid extends Validation
final case class Invalid(reason: String) extends Validation

sealed trait ReceptionistMessages

/**
 * Validate job configuration, sent from a particular client, matched to a 
 * job id. Validation action and result are stored in actions variable.
 * @param jobId is an id for a specific job
 * @param jobConf is a job configuration created by receptionist.
 * @param client reference of a particular user.
 */
final case class Validate(jobId: BSPJobID, 
                          jobConf: HamaConfiguration,
                          client: ActorRef, 
                          receptionist: ActorRef, 
                          actions: Map[Any, Validation]) 
extends ReceptionistMessages {

  def validated(): Validated = Validated(jobId, jobConf, client, actions)

}

final case class Validated(jobId: BSPJobID, 
                           jobConf: HamaConfiguration,
                           client: ActorRef, 
                           actions: Map[Any, Validation]) 
extends ReceptionistMessages 

final case object CheckMaxTasksAllowed
final case object IfTargetGroomsExist

object Reject {

  def apply(reason: String): Reject = {
    val reject = new Reject
    reject.r = reason
    reject
  }

}

/**
 * Reject is used to notify the remote client's job configuration is not valid.
 */
final class Reject extends Writable with ReceptionistMessages {

  private var r = ""

  def reason(): String = r

  @throws(classOf[IOException])
  override def write(out: DataOutput) {
    Text.writeString(out, r) 
  }

  @throws(classOf[IOException])
  override def readFields(in: DataInput) {
    r = Text.readString(in)
  }
}

object Receptionist {

  def simpleName(conf: HamaConfiguration): String = conf.get(
    "master.receptionist.name",
    classOf[Receptionist].getSimpleName
  )

  val SPLIT_FILE_HEADER: Array[Byte] = "SPL".getBytes
  val CURRENT_SPLIT_FILE_VERSION: Int = 0

  def matched(header: Array[Byte], version: Int): Boolean = {
    var flag = true
    if(!Arrays.equals(SPLIT_FILE_HEADER, header)) flag = false
    if (CURRENT_SPLIT_FILE_VERSION != version) flag = false
    flag
  }

}

/**
 * - Receive job submission from clients.
 * - Validate job configuration.
 * - Put the valid job to the wait queue.
 * - Reject back to the client if invalid.
 * @param setting contains groom related setting.
 * @param federator validates job configuration.
 */
class Receptionist(setting: Setting, federator: ActorRef) extends LocalService {

  import Receptionist._

  type JobFile = String
  type GroomServerName = String
  type MaxTasksPerGroom = Int

  /* Initialized job */
  protected var waitQueue = Queue.empty[Job]

  /* Operation against underlying storage. may need reload. */
  protected val operation = Operation.get(setting.hama)

  /**
   * Clients call submit a jobId and jobFile, where the jobFile is the
   * the path pointed to job.xml submitted.
   */
  protected def submitJob: Receive = {
    case Submit(jobId, jobFilePath) => {
      LOG.info("Received job {} submitted from the client {}", jobId,  
               sender.path.name) 
      val jobConf = newJobConf(jobId, jobFilePath)
      federator ! Validate(jobId, jobConf, sender, self,
                           Map(CheckMaxTasksAllowed -> NotVerified,
                               IfTargetGroomsExist -> NotVerified))
    }
  }
  
  protected def newJobConf(jobId: BSPJobID, 
                           jobFilePath: String): HamaConfiguration = {
    val jobConf = new HamaConfiguration() 
    val localJobFilePath = mkLocalPath(jobId, jobConf)
    LOG.info("Local job file path is at {}", localJobFilePath)
    copyJobFile(jobId)(jobFilePath)(localJobFilePath)
    jobConf.addResource(new Path(localJobFilePath))
    jobConf
  }

  /**
   * Receive validated result from federator.
   */
  protected def validated: Receive = {
    case Validated(jobId, jobConf, client, actions) => {
      val notValid = actions.filterNot { action => action._2.equals(Valid) }
      notValid.size match {
        case 0 => newJob(jobId, jobConf) match {
          case Success(newJob) => waitQueue = waitQueue.enqueue(newJob)
          case Failure(cause) => client ! Reject(cause.getMessage)
        }
        case _ => client ! Reject(notValid.head._2.asInstanceOf[Invalid].reason)
      }
    }
  }

  protected def newJob(jobId: BSPJobID, 
                       jobConf: HamaConfiguration): Try[Job] = try {
    val splits = findSplitsBy(jobId, jobConf)
    val job = new Job.Builder().setId(jobId). 
                              setConf(jobConf).
                              //withTaskTable(splits). // TODO: test when null
                              build
    Success(job)
  } catch {
    case e: Exception => Failure(e)
  }

  protected def op(jobId: BSPJobID): Operation = {
    val path = new Path(operation.getSystemDirectory, jobId.toString)
    operation.operationFor(path)
  }

  protected def op(path: Path): Operation = operation.operationFor(path)

  /**
   * Copy the job file from jobFilePath to localJobFilePath at local disk.
   * @param jobId denotes which job the action is operated.
   * @param jobFilePath indicates the remote job file path.
   * @param localJobFilePath is the dest of local job file path.
   */
  protected def copyJobFile(jobId: BSPJobID)(jobFilePath: String)
                           (localJobFilePath: String) = {
    op(jobId).copyToLocal(new Path(jobFilePath))(new Path(localJobFilePath))
  }

  /**
   * Retrieve job split files' path.
   * @param config contains user supplied information.
   * @return Option[String] if Some(path) when split file is found; otherwise
   *                        None is returned.
   */
  protected def jobSplitFilePath(config: HamaConfiguration): Option[String] = 
    config.get("bsp.job.split.file") match {
      case null => None
      case path: String => Some(path)
  }

  /**
   * Create splits according to job id and configuration provided. Split files
   * generated only contains related information without actual bytes content.
   * @param jobId denotes for which job the splits will be created.
   * @param jobConf contains user supplied information.
   * @return Option[Array[PartitionedSplit]] are splits files; or None if
   *                                         no splits.
   */
  protected def findSplitsBy(jobId: BSPJobID, 
                   jobConf: HamaConfiguration): Array[PartitionedSplit] = {
    val path = jobSplitFilePath(jobConf).getOrElse(null) 
    LOG.info("Recreate split file from {}", path)
    val input = op(operation.getSystemDirectory).open(new Path(path))
    splitsFrom(new DataInputStream(input)) 
  }

  /**
   * Read split from a particular data input, and recreate as PartitionedSplit.
   */
  protected def splitsFrom(in: DataInput): Array[PartitionedSplit] = {
    val header = new Array[Byte](SPLIT_FILE_HEADER.length)
    in.readFully(header)
    val version = WritableUtils.readVInt(in)
    matched(header, version) match {
      case true => {
        val splitLength = WritableUtils.readVInt(in) 
        val splits = new Array[PartitionedSplit](splitLength)
        for(idx <- 0 until splitLength) {
          val split = new PartitionedSplit
          split.readFields(in)
          split.isDefaultPartitionId match {
            case true => splits(split.partitionId) = split 
            case false => splits(idx) = split
          }
        }
        splits
      }
      case false => throw new RuntimeException("Split header "+header+" or "+
                                               "version "+version+" not "+
                                               "matched!")
    }
  }

  /**
   * Create corresponded path and directories. 
   * @param jobId denotes which job the operation will be applied.
   * @param config is the configuration object for the jobId supplied.
   * @return localJobFilePath points to the job file path at local.
   */
  protected def mkLocalPath(jobId: BSPJobID, 
                            config: HamaConfiguration): String = {
    val localDir = config.get("bsp.local.dir", "/tmp/bsp/local")
    val subDir = config.get("bsp.local.dir.sub_dir", setting.name)
    if(!operation.local.exists(new Path(localDir, subDir)))
      operation.local.mkdirs(new Path(localDir, subDir))
    val localJobFilePath = "%s/%s/%s.xml".format(localDir, subDir, jobId)
    localJobFilePath
  }

  /**
   * Scheduler asks for job computation.
   * Dispense a job to Scheduler.
   */
  protected def takeFromWaitQueue: Receive = {
    case TakeFromWaitQueue => {
      if(!waitQueue.isEmpty) {
        val (job, rest) = waitQueue.dequeue
        waitQueue = rest 
        LOG.info("Dispense a job {}. Now {} jobs left in wait queue.", 
                 job.getName, waitQueue.size)
        sender ! Dispense(job) // sender is scheduler
      } else LOG.warning("{} jobs in wait queue", waitQueue.size)
    }
  }

  override def receive = submitJob orElse validated orElse takeFromWaitQueue orElse unknown

}
