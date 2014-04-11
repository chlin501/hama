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

import akka.actor._
import akka.event._
import akka.testkit._
import java.util.Random
import java.io._
import org.apache.commons.io.FileUtils
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hama._
import org.apache.hama.fs._
import org.apache.hama.groom._
import org.apache.hama.bsp.BSPJobID
import org.apache.hama.bsp.v2._
import org.apache.hama.bsp.v2.IDCreator._
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

case class GetJob(ref: ActorRef)
case class JobData1(jobId: BSPJobID, localJarFile: String, localJobFile: String)

class MockMaster(conf: HamaConfiguration) extends Master(conf) {

  override def initializeServices {
    create("storage", classOf[MockStorage])
    create("receptionist", classOf[MockReceptionist])
  }
  
}

class MockReceptionist(conf: HamaConfiguration) extends Receptionist(conf) {

  override val LOG = Logging(context.system, this)

  override def notifyJobSubmission {
    LOG.info("Request sched to pull job from waitQueue.")
  }

  def getJob: Receive = {
    case GetJob(ref) => {
      if(waitQueue.isEmpty) 
        throw new NullPointerException("Job is not enqueued for waitQueue is "+
                                       "empty!");
      val job = waitQueue.dequeue._1
      LOG.info("GetJob: {}", job)
      ref ! JobData1(job.getId, job.getLocalJarFile, job.getLocalJobFile)
    }
  }
  
  override def receive = getJob orElse super.receive

}

@RunWith(classOf[JUnitRunner])
class TestReceptionist extends TestKit(ActorSystem("TestReceptionist")) 
                                    with FunSpecLike 
                                    with ShouldMatchers 
                                    with BeforeAndAfterAll {

  val LOG = LogFactory.getLog(classOf[TestReceptionist])
  val prob = TestProbe()
  val conf = new HamaConfiguration
  val fs = new MockFileSystem(conf)
  var receptionist: ActorRef = _
  val random = new Random()

  override protected def afterAll {
    system.shutdown
  }

  @throws(classOf[Exception])
  def createJobFile: String = {
    LOG.info("FileSystem: "+fs)
    val subDir = submitJobDir 
    createIfAbsent(subDir)
    val submitJobFile = new Path(subDir, "job.xml")
    LOG.info("submitJobFile: "+submitJobFile)
    var path = submitJobFile.makeQualified(fs).toString    
    path = path.replaceAll("file:", "")
    LOG.info("Path string of submitJobFile: "+path)
    var out: FileOutputStream = null
    try {
      val file = new File(path)
      out = new FileOutputStream(file, false)
      conf.writeXml(out);
    } finally {
      LOG.info("OutputStream is null? "+out)
      out.close();
    }
    path
  }

  def submitJobDir: Path = {
    new Path(getSystemDir, 
             "submit_" + Integer.toString(Math.abs(random.nextInt), 36))
  }

  def getSystemDir: Path = {
    val sysDir = new Path(conf.get("bsp.system.dir", "/tmp/hadoop/bsp/system"))
    createIfAbsent(sysDir)
    fs.makeQualified(sysDir)
  }

  def createIfAbsent(path: Path) = if(!fs.exists(path)) {
    LOG.info("Create path "+path)
    fs.mkdirs(path)
  }

  def createJobId(): BSPJobID = 
    IDCreator.newBSPJobID.withId("test_receptionist").withId(1533).build

  it("test submit job to receptionist") {
    LOG.info("Test submit job to Receptionist")
    val master = system.actorOf(Props(classOf[MockMaster], conf), "bspmaster")
    val jobId = createJobId
    val jobFile = createJobFile
    LOG.info("Submit job id "+jobId.toString+" job.xml: "+jobFile)
    master ! Request("receptionist", Submit(jobId, jobFile))
    LOG.info("Wait 5 secs ...")
    Thread.sleep(5*1000)
    master ! Request("receptionist", GetJob(prob.ref))
    prob.expectMsg(
      JobData1(jobId, 
               "/tmp/local/bspmaster/job_test_receptionist_1533.jar", 
               "/tmp/local/bspmaster/job_test_receptionist_1533.xml")
    )
  }
}
