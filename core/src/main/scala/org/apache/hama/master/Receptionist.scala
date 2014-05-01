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

  /* Initialized job */
  protected var waitQueue = Queue[Job]()

  /* Operation against underlying storage. */
  private val operation = Operation.create(configuration)
 
  override def configuration: HamaConfiguration = conf

  override def name: String = "receptionist"

  def request(to: ActorRef, message: Any) {
    import context.dispatcher
    context.system.scheduler.schedule(0.seconds, 2.seconds, to, message)
  }

  /**
   * BSPJobClient calls submitJob(jobId, jobFile), where jobFile submitted is
   * the actual job.xml content.
   *
   * This can be seen as entry point of entire procedure.
   */
  def submitJob: Receive = {
    case Submit(jobId: BSPJobID, jobFile: String) => {
      LOG.info("Received job {} submitted from the client {}",
               jobId, sender.path.name) 
      val job = initializeJob(jobId, jobFile)
      waitQueue = waitQueue.enqueue(job)
      LOG.info("{} jobs are stored in waitQueue.", waitQueue.size)
    }
  }

  def initializeJob(jobId: BSPJobID, jobFile: String): Job = {
    val (localJobFile, localJarFile) = createLocalData(jobId)
    LOG.info("localJobFile: {}, localJarFile: {}", localJobFile, localJarFile)
    copyJobFile(jobId, jobFile, localJobFile)
    addToConfiguration(localJobFile)
    copyJarFile(jobId, jarFile, localJarFile)
    val splits = createSplits(jobId)
    LOG.info("Job with id {} is created!", jobId)
    new Job.Builder().setId(jobId).
                      setConf(configuration).
                      setLocalJobFile(localJobFile).
                      setLocalJarFile(localJarFile).
                      withTaskTable(splits.getOrElse(null)).
                      build
  }

  def op(jobId: BSPJobID): Operation = {
    val path = new Path(operation.getSystemDirectory, jobId.toString)
    operation.operationFor(path)
  }

  def op(path: Path): Operation = operation.operationFor(path)

  def copyJobFile(jobId: BSPJobID, jobFile: String, localJobFile: String) = {
    op(jobId).copyToLocal(new Path(jobFile))(new Path(localJobFile))
  }

  def jarFile: Option[String] = configuration.get("bsp.jar") match {
    case null => None
    case jar@_ => Some(jar)
  }

  def copyJarFile(jobId: BSPJobID, jarFile: Option[String],
                  localJarFile: String) = jarFile match {
    case None => LOG.warning("jarFile for {} is not found!", jobId)
    case Some(jar) => {
      LOG.info("Copy jar file from {} to {}", jar, localJarFile)
      op(jobId).copyToLocal(new Path(jar))(new Path(localJarFile))
    }
  }

  def addToConfiguration(localJobFile: String) = 
    configuration.addResource(new Path(localJobFile))

  def jobSplitFile: Option[String] = 
    configuration.get("bsp.job.split.file") match {
    case null => None
    case path: String => Some(path)
  }

  def createSplits(jobId: BSPJobID): Option[Array[BSPJobClient.RawSplit]] = {
    val jobSplit = jobSplitFile 
    val splitsCreated = jobSplit match {
      case Some(path) => {
        LOG.info("Create split file from {}", path)
        val splitFile = op(operation.getSystemDirectory).open(new Path(path))
        var splits: Array[BSPJobClient.RawSplit] = null
        try {
          splits = BSPJobClient.readSplitFile(new DataInputStream(splitFile))
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

  def createLocalData(jobId: BSPJobID): (String, String) = {
    val localDir = configuration.get("bsp.local.dir", "/tmp/local")
    val subDir = configuration.get("bsp.local.dir.sub_dir", "bspmaster")
    if(!operation.local.exists(new Path(localDir, subDir)))
      operation.local.mkdirs(new Path(localDir, subDir))
    val localJobFile = "%s/%s/%s.xml".format(localDir, subDir, jobId)
    val localJarFile = "%s/%s/%s.jar".format(localDir, subDir, jobId)
    (localJobFile, localJarFile)
  }

  /**
   * Scheduler asks for job computation.
   * Dispense a job to Scheduler.
   */
  def take: Receive = {
    case Take => {
      if(!waitQueue.isEmpty) {
        val (job, rest) = waitQueue.dequeue
        waitQueue = rest 
        LOG.info("Dispense a job {}. Now {} jobs left in wait queue.", 
                 job.getName, waitQueue.size)
        sender ! Dispense(job) // sender is scheduler
      } else LOG.warning("{} jobs in wait queue", waitQueue.size)
    }
  }

  override def receive = isServiceReady orElse serverIsUp orElse take orElse submitJob orElse unknown

}
