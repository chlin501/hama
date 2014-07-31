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
import org.apache.hadoop.fs.Path
import org.apache.hama.bsp.BSPJobClient
import org.apache.hama.bsp.BSPJobClient.RawSplit
import org.apache.hama.bsp.BSPJobID
import org.apache.hama.bsp.v2.Job
import org.apache.hama.fs.Operation
import org.apache.hama.HamaConfiguration
import org.apache.hama.io.PartitionedSplit
import org.apache.hama.LocalService
import org.apache.hama.Request
import scala.collection.immutable.Queue
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

/**
 * Receive job submission from clients and put the job to the wait queue.
 * @param conf contains specific setting for the system.
 */
class Receptionist(conf: HamaConfiguration) extends LocalService {

  type JobFile = String
  type GroomServerName = String
  type MaxTasksPerGroom = Int

  /* Initialized job */
  protected var waitQueue = Queue[Job]()

  /* Simple information of GroomServer as key value pair. */
  protected var groomsStat = Map.empty[GroomServerName, MaxTasksPerGroom]
  var maxTasksSum: Int = 0

  /* Operation against underlying storage. may need reload. */
  protected val operation = Operation.get(configuration)
 
  override def configuration: HamaConfiguration = conf

  override def name: String = "receptionist"

  /**
   * Calculate maxTasks of all GroomServers.
   * @return Int the number of slots for all GroomServers.
   */
  def sumOfMaxTasks: Int = {
    var sum = 0
    groomsStat.values.foreach(v=> sum+=v)
    sum
  }

  /**
   * BSPJobClient calls submitJob(jobId, jobFile), where jobFile submitted is
   * the job.xml path.
   *
   * This can be seen as entry point of entire procedure.
   */
  def submitJob: Receive = {
    case Submit(jobId: BSPJobID, jobFilePath: String) => {
      LOG.info("Received job {} submitted from the client {}",
               jobId, sender.path.name) 
      initializeJob(jobId, jobFilePath) match {
        case Some(newJob) => {
          waitQueue = waitQueue.enqueue(newJob)
          LOG.info("{} jobs are stored in waitQueue.", waitQueue.size)
        }
        case None => {
          LOG.info("{} is rejected due to invalid requested target groom "+
                   "servers!", jobId)
          sender ! Invalid(jobId, jobFilePath) 
        }      
      }
    }
  }

  /**
   * Initialize job provided with {@link BSPJobID} and jobFile from the client.
   * @param jobId is the id of the job to be created.
   * @param jobFilePath is the path pointed to client's jobFile.
   * @param Option[Job] returns some if a job is initialized successfully, 
   *                    otherwise none.
   */
  def initializeJob(jobId: BSPJobID, jobFilePath: String): Option[Job] = {
    val config = new HamaConfiguration() 
    val localJobFilePath = createLocalPath(jobId, config)
    LOG.info("localJobFilePath is at {}", localJobFilePath)
    copyJobFile(jobId, jobFilePath, localJobFilePath)
    config.addResource(new Path(localJobFilePath)) // user provided config
    val splits = createSplits(jobId, config)
    LOG.info("Creating job with id {}!...", jobId)
    adjustNumBSPTasks(jobId, config)
    if(!validateRequestedTargets(jobId, config)) {
      None
    } else {
      Some(new Job.Builder().setId(jobId). 
                             setConf(config).
                             withTaskTable(splits.getOrElse(null)). 
                             build)
    }
  }

  /**
   * Validate if requested target array, set in "bsp.sched.targets.grooms", 
   * exceeds a single GroomServer's maxTasks allowed. 
   * @return Boolean denotes if user requested groom servers count > maxTasks;
   *                 true if request is invalid; otherwise false.
   */
  def validateRequestedTargets(jobId: BSPJobID, config: HamaConfiguration): 
      Boolean = {
    var valid = true
    val targets = config.getStrings("bsp.sched.targets.grooms")
    if(null != targets) { 
      val trimmed = targets.map{ v => v.trim }
      val groomToRequestedCount = trimmed.groupBy(k=>k).mapValues{ v=> v.size }
      LOG.debug("Mapping from groom to requested count: {}", 
               groomToRequestedCount)
      groomToRequestedCount.takeWhile{ case (groom, requestedSlotCount)=> {
        val maxTasksAllowedPerGroom = groomsStat.getOrElse(groom, 0)
        LOG.debug("User requests {} tasks for groom {}, and the max tasks "+
                 "allowed is {}", requestedSlotCount, groom, 
                 maxTasksAllowedPerGroom)
        if(requestedSlotCount > maxTasksAllowedPerGroom) valid = false
        valid
      }}
    }
    valid
  }

  /**
   * Adjust the number of BSP tasks created by the user when the value is 
   * larger than maxTasks available of the cluster.
   * @param jobId identify the client for the job.
   * @param config contains bsp configured by the user.
   */
  def adjustNumBSPTasks(jobId: BSPJobID, config: HamaConfiguration) {
    val numOfBSPTasks = config.getInt("bsp.peers.num", 1)
    if(numOfBSPTasks > maxTasksSum) {
      LOG.warning("Sum of maxTasks {} < {} for job id {} ", maxTasksSum, 
                  numOfBSPTasks, jobId.toString)
      config.setInt("bsp.peers.num", maxTasksSum)
    } 
  }

  def op(jobId: BSPJobID): Operation = {
    val path = new Path(operation.getSystemDirectory, jobId.toString)
    operation.operationFor(path)
  }

  def op(path: Path): Operation = operation.operationFor(path)

  /**
   * Copy the job file from jobFilePath to localJobFilePath at local disk.
   * @param jobId denotes which job the action is operated.
   * @param jobFilePath indicates the remote job file path.
   * @param localJobFilePath is the dest of local job file path.
   */
  def copyJobFile(jobId: BSPJobID, jobFilePath: String, 
                  localJobFilePath: String) = {
    op(jobId).copyToLocal(new Path(jobFilePath))(new Path(localJobFilePath))
  }

  /**
   * Retrieve job split files' path.
   * @param config contains user supplied information.
   * @return Option[String] if Some(path) when split file is found; otherwise
   *                        None is returned.
   */
  def jobSplitFilePath(config: HamaConfiguration): Option[String] = 
    config.get("bsp.job.split.file") match {
      case null => None
      case path: String => Some(path)
  }

  /**
   * Create splits according to job id and configuration provided. Split files
   * generated only contains related information without actual bytes content.
   * @param jobId denotes for which job the splits will be created.
   * @param config contains user supplied information.
   * @return Option[Array[PartitionedSplit]] are splits files; or None if
   *                                         no splits.
   */
  def createSplits(jobId: BSPJobID, config: HamaConfiguration): 
      Option[Array[PartitionedSplit]] = {

    val jobSplitPath = jobSplitFilePath(config) 
    val splitsCreated = jobSplitPath match {
      case Some(path) => {
        LOG.info("Create split file from {}", path)
        val splitFile = op(operation.getSystemDirectory).open(new Path(path))
        var splits: Array[PartitionedSplit] = null
        try {
          splits = BSPJobClient.asPartitionedSplit(new DataInputStream(
                   splitFile)) 
        } finally {
          splitFile.close()
        }
        
        Some(splits)
      }
      case None => None
    }
    LOG.debug("Split created for {} is {}", jobId, splitsCreated)
    splitsCreated
  }

  /**
   * Create required path and directories. 
   * @param jobId denotes which job the operation will be applied.
   * @param config is the configuration object for the jobId supplied.
   * @return localJobFilePath points to the job file path at local.
   */
  def createLocalPath(jobId: BSPJobID, config: HamaConfiguration): String = {
    val localDir = config.get("bsp.local.dir", "/tmp/local")
    val subDir = config.get("bsp.local.dir.sub_dir", "bspmaster")
    if(!operation.local.exists(new Path(localDir, subDir)))
      operation.local.mkdirs(new Path(localDir, subDir))
    val localJobFilePath = "%s/%s/%s.xml".format(localDir, subDir, jobId)
    localJobFilePath
  }

  /**
   * Scheduler asks for job computation.
   * Dispense a job to Scheduler.
   */
  def takeFromWaitQueue: Receive = {
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

  /**
   * From GroomManager to notify a groom server's maxTasks.
   */
  def updateGroomStat: Receive = {
    case GroomStat(groomServerName, maxTasks) => {
      groomsStat ++= Map(groomServerName -> maxTasks)
      maxTasksSum = sumOfMaxTasks
      LOG.info("Sum up of all GroomServers maxTasks is {}", maxTasksSum)
    }
  }

  override def receive = submitJob orElse takeFromWaitQueue orElse updateGroomStat orElse isServiceReady orElse mediatorIsUp orElse unknown

}
