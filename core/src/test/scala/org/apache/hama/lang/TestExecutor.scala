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
package org.apache.hama.lang

import akka.actor._
import akka.event._
import akka.testkit._
import java.io._
import org.apache.commons.io.FileUtils
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import org.apache.hama._
import org.apache.hama.groom._
import org.apache.hama.bsp.BSPJobID
import org.apache.hama.bsp.TaskAttemptID
import org.apache.hama.bsp.v2._
import org.apache.hama.bsp.v2.IDCreator._
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

final case object GetProcessParam
final case class ProcessParam(cmd: String, workDir: String, 
                              logDir: String)

class MockExecutor(conf: HamaConfiguration, ref: ActorRef) 
    extends Executor(conf) {

  var command: Seq[String] = _
  var workingDirectory: File = _
  var loggingDirectory: File = _

  override def logPath: String = "/tmp/hama/logs"
  override def javacp: String = ""

  override def defaultOpts: String = "-Xmx200m"

  override def unjar(jarPath: String, workDir: File) {
    if(!workDir.exists) workDir.mkdirs
    new File(workDir, "lib")
  }

  override def libsPath(workDir: File): String = 
    "/tmp/hama/job/work/libs/a:/tmp/hama/job/work/libs/c:/tmp/hama/job/work/libs/b"

  override def createProcess(cmd: Seq[String], workDir: File, logDir: File) {
    command = cmd
    LOG.info("command: {}", command)
    workingDirectory = workDir
    LOG.info("workingDirectory: {}", workingDirectory)
    loggingDirectory = logDir
    LOG.info("loggingDirectory: {}", loggingDirectory)
  }

  def getProcessParam: Receive = {
    case GetProcessParam => {
      LOG.info("command: {} workingDir: {} loggingDir: {}", 
               command.mkString(","), workingDirectory, loggingDirectory)
      ref ! ProcessParam(command.mkString(","), workingDirectory.getPath, 
                            loggingDirectory.getPath)
    }
  }

  override def receive = getProcessParam orElse super.receive
  
}

@RunWith(classOf[JUnitRunner])
class TestExecutor extends TestKit(ActorSystem("TestExecutor")) 
                                    with FunSpecLike 
                                    with ShouldMatchers 
                                    with BeforeAndAfterAll {
  val content: String = """
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

  val LOG = LogFactory.getLog(classOf[TestExecutor])
  val prob = TestProbe()
  val conf = new HamaConfiguration
  var executor: ActorRef = _
  val root: File = {
    val f = new File("/tmp/hama") 
    if(!f.exists) f.mkdirs
    f
  }
  val tmpJobFile = {
    File.createTempFile("fork_", ".xml", root)
  }

  override protected def afterAll {
    if(root.exists) {
      LOG.info("Delete root path: "+root.getPath)
      FileUtils.deleteDirectory(root)
    }
    system.shutdown
  }

  def createJobId: BSPJobID = 
    IDCreator.newBSPJobID.withId("test_fork_process").withId(1234).build

  @throws(classOf[Exception])
  def createJobFile: File = {
    FileUtils.writeStringToFile(tmpJobFile, content) 
    tmpJobFile
  }

  def createJarPath: String = new File(root, "test.jar").getPath

  def createTaskAttemptId(jobId: BSPJobID): TaskAttemptID = {
    IDCreator.newTaskID.withId(jobId)
                       .withId(3)
                       .getTaskAttemptIDBuilder
                       .withId(2)
                       .build
  }

  it("test fork a process") {
    LOG.info("Test fork a process.")
    executor = system.actorOf(Props(classOf[MockExecutor], conf, prob.ref), 
                                  "executor")
    val jobId = createJobId
    val jobFilePath = createJobFile.getPath
    val jarPath = createJarPath 
    val taskAttemptId = createTaskAttemptId(jobId)
    val superstep = 7
    LOG.info("BSPJobId: "+jobId+" jobFilePath: "+jobFilePath+
             " jarPath: "+jarPath +" taskAttemptId: "+taskAttemptId)
    executor ! Fork(jobId.toString, jobFilePath, jarPath, 
                    taskAttemptId.toString, superstep)
    executor ! GetProcessParam
    
    val cmd = System.getProperty("java.home")+"/"+
              "bin/java,-Xmx200m,"+
              "org.apache.hama.lang.BSPChild,127.0.0.1,50001,"+
              "attempt_test_fork_process_1234_000003_2,"+superstep
    val workDir = "/tmp/hama/work"
    val logDir = "/tmp/hama/logs/tasklogs/job_test_fork_process_1234"
    prob.expectMsg(ProcessParam(cmd, workDir, logDir))
  }
}
