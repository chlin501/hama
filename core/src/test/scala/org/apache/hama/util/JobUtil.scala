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
package org.apache.hama.util

import java.util.Random
import java.io.File
import java.io.FileOutputStream
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hama.bsp.BSPJobID
import org.apache.hama.bsp.TaskAttemptID
import org.apache.hama.bsp.v2.Job
import org.apache.hama.bsp.v2.Task
import org.apache.hama.bsp.v2.IDCreator
import org.apache.hama.bsp.v2.IDCreator._
import org.apache.hama.fs.Operation
import org.apache.hama.fs.HDFS
import org.apache.hama.HamaConfiguration
import org.apache.hama.logging.Logger

class MockHDFS extends HDFS {

  def getHDFS: FileSystem = hdfs

}

trait JobUtil extends Logger {

  val sampleJobFileContent: String = """
<?xml version="3.0" encoding="UTF-8" standalone="no"?><configuration>
<property><!--Loaded from core-default.xml--><name>fs.file.impl</name><value>org.apache.hadoop.fs.LocalFileSystem</value></property>
<property><!--Loaded from core-default.xml--><name>fs.webhdfs.impl</name><value>org.apache.hadoop.hdfs.web.WebHdfsFileSystem</value></property>
<property><!--Loaded from core-default.xml--><name>hadoop.logfile.count</name><value>10</value></property>
<property><!--Loaded from core-default.xml--><name>fs.har.impl.disable.cache</name><value>true</value></property>
<property><!--Loaded from core-default.xml--><name>hadoop.http.authentication.kerberos.keytab</name><value>${user.home}/hadoop.keytab</value></property>
<property><!--Loaded from core-default.xml--><name>ipc.client.kill.max</name><value>10</value></property>
<property><!--Loaded from core-default.xml--><name>fs.s3n.impl</name><value>org.apache.hadoop.fs.s3native.NativeS3FileSystem</value></property>
<property><!--Loaded from core-default.xml--><name>hadoop.security.token.service.use_ip</name><value>true</value></property>
<property><!--Loaded from core-default.xml--><name>io.mapfile.bloom.size</name><value>1048576</value></property>
<property><!--Loaded from core-default.xml--><name>fs.s3.sleepTimeSeconds</name><value>10</value></property>
<property><!--Loaded from core-default.xml--><name>fs.s3.block.size</name><value>67108864</value></property>
<property><!--Loaded from core-default.xml--><name>fs.kfs.impl</name><value>org.apache.hadoop.fs.kfs.KosmosFileSystem</value></property>
<property><!--Loaded from core-default.xml--><name>hadoop.skip.worker.version.check</name><value>false</value></property>
<property><!--Loaded from core-default.xml--><name>ipc.server.listen.queue.size</name><value>128</value></property>
<property><!--Loaded from core-default.xml--><name>hadoop.util.hash.type</name><value>murmur</value></property>
<property><!--Loaded from core-default.xml--><name>hadoop.http.authentication.kerberos.principal</name><value>HTTP/localhost@LOCALHOST</value></property>
<property><!--Loaded from core-default.xml--><name>hadoop.http.authentication.simple.anonymous.allowed</name><value>true</value></property>
<property><!--Loaded from core-default.xml--><name>ipc.client.tcpnodelay</name><value>false</value></property>
<property><!--Loaded from core-default.xml--><name>io.file.buffer.size</name><value>4096</value></property>
<property><!--Loaded from core-default.xml--><name>fs.s3.buffer.dir</name><value>${hadoop.tmp.dir}/s3</value></property>
<property><!--Loaded from core-default.xml--><name>hadoop.tmp.dir</name><value>/tmp/hadoop-${user.name}</value></property>
<property><!--Loaded from core-default.xml--><name>fs.trash.interval</name><value>0</value></property>
<property><!--Loaded from core-default.xml--><name>io.seqfile.sorter.recordlimit</name><value>1000000</value></property>
<property><!--Loaded from core-default.xml--><name>fs.ftp.impl</name><value>org.apache.hadoop.fs.ftp.FTPFileSystem</value></property>
<property><!--Loaded from core-default.xml--><name>fs.checkpoint.size</name><value>67108864</value></property>
<property><!--Loaded from core-default.xml--><name>fs.checkpoint.period</name><value>3600</value></property>
<property><!--Loaded from core-default.xml--><name>fs.hftp.impl</name><value>org.apache.hadoop.hdfs.HftpFileSystem</value></property>
<property><!--Loaded from core-default.xml--><name>hadoop.relaxed.worker.version.check</name><value>false</value></property>
<property><!--Loaded from core-default.xml--><name>hadoop.native.lib</name><value>true</value></property>
<property><!--Loaded from core-default.xml--><name>fs.hsftp.impl</name><value>org.apache.hadoop.hdfs.HsftpFileSystem</value></property>
<property><!--Loaded from core-default.xml--><name>ipc.client.connect.max.retries</name><value>10</value></property>
<property><!--Loaded from core-default.xml--><name>fs.har.impl</name><value>org.apache.hadoop.fs.HarFileSystem</value></property>
<property><!--Loaded from core-default.xml--><name>fs.s3.maxRetries</name><value>4</value></property>
<property><!--Loaded from core-default.xml--><name>hadoop.http.authentication.signature.secret.file</name><value>${user.home}/hadoop-http-auth-signature-secret</value></property>
<property><!--Loaded from core-default.xml--><name>topology.node.switch.mapping.impl</name><value>org.apache.hadoop.net.ScriptBasedMapping</value></property>
<property><!--Loaded from core-default.xml--><name>hadoop.logfile.size</name><value>10000000</value></property>
<property><!--Loaded from core-default.xml--><name>hadoop.http.authentication.token.validity</name><value>36000</value></property>
<property><!--Loaded from core-default.xml--><name>fs.checkpoint.dir</name><value>${hadoop.tmp.dir}/dfs/namesecondary</value></property>
<property><!--Loaded from core-default.xml--><name>fs.checkpoint.edits.dir</name><value>${fs.checkpoint.dir}</value></property>
<property><!--Loaded from core-default.xml--><name>topology.script.number.args</name><value>100</value></property>
<property><!--Loaded from core-default.xml--><name>fs.s3.impl</name><value>org.apache.hadoop.fs.s3.S3FileSystem</value></property>
<property><!--Loaded from core-default.xml--><name>ipc.client.connection.maxidletime</name><value>10000</value></property>
<property><!--Loaded from core-default.xml--><name>io.compression.codecs</name><value>org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.BZip2Codec,org.apache.hadoop.io.compress.SnappyCodec</value></property>
<property><!--Loaded from core-default.xml--><name>hadoop.security.uid.cache.secs</name><value>14400</value></property>
<property><!--Loaded from core-default.xml--><name>ipc.server.tcpnodelay</name><value>false</value></property>
<property><!--Loaded from core-default.xml--><name>io.serializations</name><value>org.apache.hadoop.io.serializer.WritableSerialization</value></property>
<property><!--Loaded from core-default.xml--><name>hadoop.security.use-weak-http-crypto</name><value>false</value></property>
<property><!--Loaded from core-default.xml--><name>hadoop.jetty.logs.serve.aliases</name><value>true</value></property>
<property><!--Loaded from core-default.xml--><name>hadoop.http.authentication.type</name><value>simple</value></property>
<property><!--Loaded from core-default.xml--><name>ipc.client.idlethreshold</name><value>4000</value></property>
<property><!--Loaded from core-default.xml--><name>fs.hdfs.impl</name><value>org.apache.hadoop.hdfs.DistributedFileSystem</value></property>
<property><!--Loaded from core-default.xml--><name>io.bytes.per.checksum</name><value>512</value></property>
<property><!--Loaded from core-default.xml--><name>hadoop.security.group.mapping</name><value>org.apache.hadoop.security.ShellBasedUnixGroupsMapping</value></property>
<property><!--Loaded from core-default.xml--><name>io.mapfile.bloom.error.rate</name><value>0.005</value></property>
<property><!--Loaded from core-default.xml--><name>io.seqfile.lazydecompress</name><value>true</value></property>
<property><!--Loaded from core-default.xml--><name>local.cache.size</name><value>10737418240</value></property>
<property><!--Loaded from core-default.xml--><name>hadoop.security.instrumentation.requires.admin</name><value>false</value></property>
<property><!--Loaded from core-default.xml--><name>hadoop.security.authorization</name><value>false</value></property>
<property><!--Loaded from core-default.xml--><name>hadoop.security.authentication</name><value>simple</value></property>
<property><!--Loaded from core-default.xml--><name>hadoop.rpc.socket.factory.class.default</name><value>org.apache.hadoop.net.StandardSocketFactory</value></property>
<property><!--Loaded from core-default.xml--><name>io.skip.checksum.errors</name><value>false</value></property>
<property><!--Loaded from core-default.xml--><name>io.seqfile.compress.blocksize</name><value>1000000</value></property>
<property><!--Loaded from core-default.xml--><name>fs.ramfs.impl</name><value>org.apache.hadoop.fs.InMemoryFileSystem</value></property>
<property><!--Loaded from core-default.xml--><name>webinterface.private.actions</name><value>false</value></property>
<property><!--Loaded from core-default.xml--><name>net.topology.impl</name><value>org.apache.hadoop.net.NetworkTopology</value></property>
<property><!--Loaded from core-default.xml--><name>fs.default.name</name><value>file:///</value></property>
</configuration>
  """
  
  val random = new Random()

  val op = Operation.get[MockHDFS](configuration, classOf[MockHDFS]).
                     asInstanceOf[MockHDFS]

  def configuration: HamaConfiguration = new HamaConfiguration

  def qualified(path: Path): Path = path.makeQualified(op.getHDFS)  

  def fsQualified(path: Path): Path = op.getHDFS.makeQualified(path)

  //* Below are Job related functions. *

  def createJobId(identifier: String, id: Int): BSPJobID =
    IDCreator.newBSPJobID.withId(identifier).withId(id).build

  def createJob(identifier: String, id: Int, jobName: String, 
                targets: Array[String], numBSPTasks: Int): Job = { 
    val jobId = createJobId(identifier, id)
    new Job.Builder().setId(jobId).
                      setName(jobName).
                      setTargets(targets).
                      setNumBSPTasks(numBSPTasks).
                      withTaskTable.
                      build
  }

  def createJob(identifier: String, id: Int, jobName: String,
                numBSPTasks: Int): Job = {
    val jobId = createJobId(identifier, id)
    new Job.Builder().setId(jobId).
                      setName(jobName).
                      setNumBSPTasks(numBSPTasks).
                      withTaskTable.
                      build
  }

  @throws(classOf[Exception])
  def createJobFile(conf: HamaConfiguration): String = {
    val subDir = getSubmitJobDir(conf)
    createIfAbsent(subDir)
    val jobFilePath = new Path(subDir, "job.xml")
    LOG.info("jobFilePath (job.xml): %s".format(jobFilePath))
    var path = qualified(jobFilePath).toString
    path = path.replaceAll("file:", "")
    LOG.info("Path string of jobFilePath: %s".format(path))
    var out: FileOutputStream = null
    try {
      val file = new File(path)
      out = new FileOutputStream(file, false)
      conf.writeXml(out);
    } finally {
      if(null != out) {
        LOG.info("Close OutputStream: %s".format(out))
        out.close();
      }
    }
    LOG.info("JobFile is created at %s".format(path))
    path
  }

  @throws(classOf[Exception])
  def createJobFile(content: String)(implicit testRoot: File): File = { 
    val tmpJobFile = File.createTempFile("temp_", ".xml", testRoot)
    FileUtils.writeStringToFile(tmpJobFile, content)
    tmpJobFile
  }

  def createTaskAttemptId(jobId: BSPJobID, taskId: Int, taskAttemptId: Int): 
      TaskAttemptID = {
    IDCreator.newTaskID.withId(jobId)
                       .withId(taskId) 
                       .getTaskAttemptIDBuilder
                       .withId(taskAttemptId)
                       .build
  }

  def createTask(jobIdentifier: String = "test", jobId: Int = 1,
                 taskId: Int = 1, taskAttemptId: Int = 1): Task = {
    val attemptId = IDCreator.newBSPJobID.withId(jobIdentifier)
                                         .withId(jobId)
                                         .getTaskIDBuilder
                                         .withId(taskId)
                                         .getTaskAttemptIDBuilder
                                         .withId(taskAttemptId)
                                         .build
    val startTime = System.currentTimeMillis
    val finishTime = startTime + 1000*10
    val state = Task.State.RUNNING
    val phase = Task.Phase.SETUP
    new Task.Builder().setId(attemptId)
                      .setStartTime(startTime)
                      .setFinishTime(finishTime)
                      .setState(state)
                      .setPhase(phase)
                      .setCompleted(true)
                      .build
  }

  def createJarPath(jar: String)(implicit testRoot: File): File = new File(testRoot, jar)

  def getSystemDir(conf: HamaConfiguration): Path = {
    val sysDir = new Path(conf.get("bsp.system.dir", "/tmp/hadoop/bsp/system"))
    createIfAbsent(sysDir)
    fsQualified(sysDir)
  }

  def createIfAbsent(path: Path) = if(!op.exists(path)) {
    LOG.info("Path %s is absent so creating ...".format(path))
    op.mkdirs(path)
  }

  def getSubmitJobDir(conf: HamaConfiguration): Path = {
    new Path(getSystemDir(conf),
             "submit_" + Integer.toString(Math.abs(random.nextInt), 36))
  }
}
