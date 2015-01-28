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
package org.apache.hama.client

import akka.actor.ActorRef
import akka.actor.Props
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import java.io.File
import java.io.IOException
import java.net.URL
import java.net.URLClassLoader
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.DoubleWritable
import org.apache.hadoop.io.Text
import org.apache.hama.HamaConfiguration
import org.apache.hama.TestEnv
import org.apache.hama.SystemInfo
import org.apache.hama.bsp.BSPJob
import org.apache.hama.bsp.BSPJobID
import org.apache.hama.bsp.FileOutputFormat
import org.apache.hama.bsp.NullInputFormat
import org.apache.hama.bsp.TextOutputFormat
import org.apache.hama.bsp.v2.BSP
import org.apache.hama.bsp.v2.BSPPeer
import org.apache.hama.conf.ClientSetting
import org.apache.hama.conf.Setting
import org.apache.hama.fs.Operation
import org.apache.hama.groom.Executor // TODO: move hamaHome to util?
import org.apache.hama.logging.CommonLog
import org.apache.hama.master.BSPMaster
import org.apache.hama.message.compress.SnappyCompressor
import org.apache.hama.sync.SyncException
import org.apache.hama.util.JobUtil
import org.apache.hama.util.Tool
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.collection.immutable.IndexedSeq

class MockM(setting: Setting, tester: ActorRef) 
      extends BSPMaster(setting, "testMaster") with JobUtil {

  val conf = new HamaConfiguration

  override def join(nodes: IndexedSeq[SystemInfo]) { } 

  override def subscribe(stakeholder: ActorRef) { }

  override def processClientRequest(from: ActorRef) = 
    tester ! Response(jobId, sysDir, 1024)

  def sysDir: Path = new Path(conf.get("bsp.system.dir", 
    "/tmp/hadoop/bsp/system"))

  def jobId: BSPJobID = createJobId("test", 1)

  override def receive = opened
} 

final case class Tester(t: ActorRef)
final case class Assign(m: ActorRef)
final case object Unassign
final case class NumBSPTasks(before: Int, after: Int)
final case class JobName(equal: Boolean)

class MockSubmitter(setting: Setting) extends Submitter(setting) {

  var tester: Option[ActorRef] = None

  override def initializeServices { /* disable retry */ }

  def testMsg: Receive = {
    case Tester(t) => tester = Option(t)
    case Assign(m) => {
      LOG.info("Call afterLinked for simulating master reply.")
      afterLinked("discover", m)
    }
    case Unassign => masterProxy = None
  }

  override def randomValue(): String = "random"

  override def workingDirs(sysDir: Path): WorkingDirs = {
    val dirs = super.workingDirs(sysDir)
    LOG.info("Actual job dir: {}, split path: {}, jar path: {}, job path: {}", 
             dirs.jobDir, dirs.splitPath, dirs.jarPath, dirs.jobPath)
    tester.map { t => t ! dirs }
    dirs
  }

  override def adjustTasks(job: BSPJob, maxTasksAllowed: Int): BSPJob = {
    val before = job.getNumBspTask
    val newJob = super.adjustTasks(job, maxTasksAllowed)
    val after = newJob.getNumBspTask
    LOG.info("Num BSP tasks => before adjusted: {}, after adjusted: {}", 
             before, after)
    tester.map { t => t ! NumBSPTasks(before, after) }
    newJob
  }

  override def copyJarToMaster(jobMayHaveSplit: BSPJob,
                               splitPath: Path,
                               operation: Operation): BSPJob = {
    val localJarPath = jobMayHaveSplit.getJar
    LOG.info("before copying jar to master, local jar path is {}", localJarPath)
    val job = super.copyJarToMaster(jobMayHaveSplit, splitPath, operation)
    val jobNameExpected = new Path(localJarPath).getName
    val jobNameActual = job.getJobName
    LOG.info("Expected job name: {}, actual job name: {}", 
             jobNameExpected, jobNameActual)
    tester.map { t => t ! JobName(jobNameExpected.equals(jobNameActual))}
    job
  }


  override def receive = testMsg orElse super.receive
   
}

object MockSetting {

  def apply(): Setting = new MockSetting(Setting.client).asInstanceOf[Setting]
}

class MockSetting(setting: Setting) extends ClientSetting(setting.hama) {

  override def config(): Config = ConfigFactory.load

}

@RunWith(classOf[JUnitRunner])
class TestSubmitter extends TestEnv("TestSubmitter") with JobUtil {

  val TMP_OUTPUT = new Path("/tmp/pi-" + System.currentTimeMillis)

  val expectedJobId = createJobId("test", 1)

  val expectedSysDir: Path = new Path("/tmp/hadoop/bsp/system")

  val expectedMaxTasks = 1024

  val clientRequestTasks = 4096


  val submitterDir = constitute(testRootPath, "submitter")
  val classesDir = constitute(submitterDir, "classes")
  val jarDir = constitute(submitterDir, "jar")
  val jarFile = constitute(jarDir, "client.jar")
  val customizedBSP = "org.apache.hama.examples.PiEstimator$MyEstimator"
  val main = "org.apache.hama.examples.PiEstimator"
  var mybsp: Option[Class[_]] = None
  var mainClass: Option[Class[_]] = None 

  override def beforeAll = {
    super.beforeAll()
    mkdirs(submitterDir)
    mkdirs(classesDir)
    mkdirs(jarDir)
    Tool.compile(sources, classesDir)
    Tool.jar(classesDir, jarFile)
    val loader = addToClasspath(jarFile) 
    mainClass = Option(Class.forName(main, true, loader))
    mybsp = Option(Class.forName(customizedBSP, true, loader))
  } 

  //override def afterAll {}

  def addToClasspath(paths: String*): ClassLoader = {
    val urls = paths.map { path => new File(path).toURI.toURL }.toArray[URL]
    LOG.info("Urls to be added: {}", urls.mkString(", "))
    val loader = Thread.currentThread.getContextClassLoader
    val newLoader = new URLClassLoader(urls, loader) 
    Thread.currentThread.setContextClassLoader(newLoader)
    newLoader
  }

  def piEstimator(): String = {
    require(null != Executor.hamaHome, "hama.home.dir is not set!")
    constitute(Executor.hamaHome, "core", "src", "test", "resources", 
               "examples", "PiEstimator.scala")
  }


  def sources(): List[String] = List(piEstimator) 

  def expectedWorkingDirs(): WorkingDirs = {
    val jobDir = new Path(expectedSysDir, "submit_random")
    val splitPath = new Path(jobDir, "job.split") 
    val jarPath = new Path(jobDir, "job.jar") 
    val jobPath = new Path(jobDir, "job.xml")
    WorkingDirs(jobDir, splitPath, jarPath, jobPath)
  }

  def configMaster(setting: Setting): Setting = {
    setting.hama.setBoolean("master.need.register", false)
    setting.hama.setBoolean("master.fs.cleaner.start", false)
    setting
  }

  def configClient(setting: Setting): Setting = {
    setting.hama.set("client.main", classOf[MockSubmitter].getName)
    setting
  }

  def bspJob(requestTasks: Int): BSPJob = mybsp match { 
    case None => throw new RuntimeException("BSP class is missing!")
    case Some(customized) => mainClass match { 
      case None => throw new RuntimeException("Main class is missing!")
      case Some(m) => {
        val conf = new HamaConfiguration
        val bsp = new BSPJob(conf, m)
        bsp.setCompressor(classOf[SnappyCompressor]) 
        bsp.setCompressionThreshold(40)
        bsp.setBSPClass(customized.asInstanceOf[Class[BSP]])
        bsp.setInputFormat(classOf[NullInputFormat])
        bsp.setOutputKeyClass(classOf[Text])
        bsp.setOutputValueClass(classOf[DoubleWritable])
        //bsp.setOutputFormat(classOf[TextOutputFormat])
        FileOutputFormat.setOutputPath(bsp, TMP_OUTPUT)
        bsp.setNumBspTask(requestTasks)
        bsp
      }
    }
  }


  it("test submitter functions.") {
    val master = configMaster(Setting.master)
    Submitter.start(configClient(MockSetting()))
    val submitter = Submitter.submitter 
    submitter.map { client => client ! Tester(tester) }
    val succeedSubmission = Submitter.submit(bspJob(clientRequestTasks))
    assert(succeedSubmission)
    Submitter.system.map { sys => 
      val m = sys.actorOf(Props(classOf[MockM], master, tester), "mockMaster")
      submitter.map { client => client ! Assign(m) }
    }
    val response = Response(expectedJobId, expectedSysDir, expectedMaxTasks)
    expect(response)
    submitter.map { client => client ! response }
    expect(expectedWorkingDirs)
    expect(NumBSPTasks(clientRequestTasks, expectedMaxTasks))
    expect(JobName(true))
    LOG.info("Done testing Submitter functions!")    
  }


}
