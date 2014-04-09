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

import akka.routing._
import org.apache.hama._
import org.apache.hama.bsp.BSPJobID
import org.apache.hama.bsp.v2.Job
import org.apache.hama.master._
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import scala.collection.immutable.Queue

/**
 * Receive job submission from clients and put the job to the wait queue.
 */
class Receptionist(conf: HamaConfiguration) extends LocalService {

  private[this] var waitQueue = Queue[Job]()
 
  override def configuration: HamaConfiguration = conf

  override def name: String = "receptionist"

  // move to trait 
  def fileSystem: FileSystem = FileSystem.get(conf)

  def localFs: FileSystem = FileSystem.getLocal(conf)

  def createLocalData(jobId: BSPJobID): (String, String) = {
    val localDir = conf.get("bsp.local.dir", "/tmp/local")
    val subDir = conf.get("bsp.local.dir.sub_dir", "bspmaster")
    if(!localFs.exists(new Path(localDir, subDir))) 
      fileSystem.mkdirs(new Path(localDir, subDir))
    val localJobFile = "%s/%s/%s.xml".format(localDir, subDir, jobId)
    val localJarFile = "%s/%s/%s.jar".format(localDir, subDir, jobId)
    (localJobFile, localJarFile)
  }

  def sysDir: String = conf.get("bsp.system.dir", "/tmp/hadoop/bsp/system")

  def systemDir: String = fileSystem.makeQualified(new Path(sysDir)).toString

  def fs(jobId: BSPJobID): FileSystem = {
    val jobDir = new Path(new Path(sysDir), jobId.toString)
    jobDir.getFileSystem(conf) 
  }

  /**
   * Copy the job.xml from a specified path, jobFile, to local, localJobFile.
   * @param jobId denotes the BSPJobID
   * @param jobFile denotes the job.xml submitted from the client.
   * @param localJobFile is the detination to which the job file to be copied.
   */
  def copyJobFile(jobId: BSPJobID, jobFile: String, localJobFile: String) =
    fs(jobId).copyToLocalFile(new Path(jobFile), new Path(localJobFile))

  def copyJarFile(jobId: BSPJobID, jarFile: Option[String], 
                  localJarFile: String) = jarFile match {
    case None => 
    case Some(jar) => 
      fs(jobId).copyToLocalFile(new Path(jar), new Path(localJarFile)) 
  }

  def addToConfiguration(localJobFile: String) {
    conf.addResource(new Path(localJobFile))
  }

  def jobSplitFile: String = conf.get("bsp.job.split.file")

  def jarFile: Option[String] = conf.get("bsp.jar") match { 
    case null => None
    case jar@_ => Some(jar)
  }

  /**
   * Perform necessary steps to initialize a Job.
   * @param jobId is a unique BSPJobID  
   * @param jobFile is submitted from a client.
   */
  def initJob(jobId: BSPJobID, jobFile: String): Job = {
    val (localJobFile, localJarFile) = createLocalData(jobId)
    copyJobFile(jobId, jobFile, localJobFile)
    addToConfiguration(localJobFile)
    val jobSplit = jobSplitFile
    copyJarFile(jobId, jarFile, localJarFile)
    new Job.Builder().setId(jobId).
                      setConf(conf).
                      setLocalJobFile(localJobFile).
                      setLocalJarFile(localJarFile).
                      withTaskTable.
                      build
  }

  /**
   * BSPJobClient call submitJob(jobId, jobFile)
   */
  def submitJob: Receive = {
    case Submit(jobId: BSPJobID, xml: String) => {
      LOG.info("Received job {} submitted from the client {}",
               jobId, sender.path.name) 
      val job = initJob(jobId, xml)
      waitQueue = waitQueue.enqueue(job)
      mediator ! Request("sched", JobSubmission)  
    }
  }

  /**
   * Dispense a job to Scheduler.
   */
  def take: Receive = {
    case Take => {
      if(!waitQueue.isEmpty) {
        val (job, rest) = waitQueue.dequeue
        waitQueue = rest 
        LOG.info("Dispense a job {}. Now {} jobs left in wait queue.", 
                 job.getName, waitQueue.size)
        sender ! Dispense(job)
      } else LOG.warning("{} jobs in wait queue", waitQueue.size)
    }
  }

  override def receive = isServiceReady orElse serverIsUp orElse take orElse submitJob orElse unknown

}
