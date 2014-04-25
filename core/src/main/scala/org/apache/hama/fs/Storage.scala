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
package org.apache.hama.fs

/*
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hama.bsp.BSPJobClient
import org.apache.hama.bsp.BSPJobClient.RawSplit
import org.apache.hama.bsp.BSPJobID
import org.apache.hama.bsp.v2.Job
import org.apache.hama.HamaConfiguration
import org.apache.hama.LocalService
import org.apache.hama.master.Enqueue

class Storage(conf: HamaConfiguration) extends LocalService {

  override def configuration: HamaConfiguration = conf

  override def name: String = "storage"

  override def initializeService = {
    // TODO: 1. get actual fs class (implented hama fs interface) from configuration 
    //       2. instantiate fs object 
  }

  def fileSystem: FileSystem = FileSystem.get(configuration)

  def localFs: FileSystem = FileSystem.getLocal(configuration)

  def createLocalData(jobId: BSPJobID): (String, String) = {
    val localDir = configuration.get("bsp.local.dir", "/tmp/local")
    val subDir = configuration.get("bsp.local.dir.sub_dir", "bspmaster")
    if(!localFs.exists(new Path(localDir, subDir)))
      fileSystem.mkdirs(new Path(localDir, subDir))
    val localJobFile = "%s/%s/%s.xml".format(localDir, subDir, jobId)
    val localJarFile = "%s/%s/%s.jar".format(localDir, subDir, jobId)
    (localJobFile, localJarFile)
  }

  def sysDir: String = 
    configuration.get("bsp.system.dir", "/tmp/hadoop/bsp/system")

  def systemDir: String = fileSystem.makeQualified(new Path(sysDir)).toString

  def fs(jobId: BSPJobID): FileSystem = {
    val jobDir = new Path(new Path(sysDir), jobId.toString)
    jobDir.getFileSystem(configuration)
  }

   * Copy the job.xml from a specified path, jobFile, to local, localJobFile.
   * @param jobId denotes the BSPJobID
   * @param jobFile denotes the job.xml submitted from the client.
   * @param localJobFile is the detination to which the job file to be copied.
  def copyJobFile(jobId: BSPJobID, jobFile: String, localJobFile: String) = {
    fs(jobId).copyToLocalFile(new Path(jobFile), new Path(localJobFile))
  }

  def copyJarFile(jobId: BSPJobID, jarFile: Option[String],
                  localJarFile: String) = jarFile match {
    case None =>
    case Some(jar) => {
      LOG.info("Copy jar file from {} to {}", jar, localJarFile)
      fs(jobId).copyToLocalFile(new Path(jar), new Path(localJarFile))
    }
  }

  def addToConfiguration(localJobFile: String) = 
    configuration.addResource(new Path(localJobFile))

  def jobSplitFile: Option[String] = 
    configuration.get("bsp.job.split.file") match {
    case null => None
    case path: String => Some(path)
  }

  def jarFile: Option[String] = configuration.get("bsp.jar") match {
    case null => None
    case jar@_ => Some(jar)
  }

  def createSplits(jobId: BSPJobID): Option[Array[BSPJobClient.RawSplit]] = {
    val jobSplit = jobSplitFile 
    val splitsCreated = jobSplit match {
      case Some(path) => {
        val fs = new Path(systemDir).getFileSystem(configuration)
        val splitFile = fs.open(new Path(path))
        var splits: Array[BSPJobClient.RawSplit] = null
        try {
          splits = BSPJobClient.readSplitFile(splitFile);
        } finally {
          splitFile.close();
        }
        Some(splits)
      }
      case None => None
    }
    LOG.debug("Split created for {} is {}", jobId, splitsCreated)
    splitsCreated
  }

   * Perform necessary steps to initialize a Job.
   * @param jobId is a unique BSPJobID
   * @param jobFile is submitted from a client.
  def initJob(jobId: BSPJobID, jobFile: String): Job = {
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

   * Send the initialized job back to {@link Receptionist}.
  def initJob: Receive = {
    case InitializeJob(jobId, xml) => {
      val job = initJob(jobId, xml)
      LOG.info("Initialized job: {} for jobId: {}", job, jobId)
      sender ! Enqueue(job)
    }
  }

  override def receive = initJob orElse isServiceReady orElse serverIsUp orElse unknown 
}
*/
