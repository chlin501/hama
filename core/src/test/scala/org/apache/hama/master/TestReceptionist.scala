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
import akka.event.Logging
import org.apache.hama.bsp.BSPJobID
import org.apache.hama.groom._
import org.apache.hama.HamaConfiguration
import org.apache.hama.Request
import org.apache.hama.TestEnv
import org.apache.hama.util.JobUtil
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

private final case class GetJob(tester: ActorRef)
private final case class JobContent(jobId: BSPJobID, 
                                    localJarFile: String/*, 
                                    localJobFile: String*/)

class MockMaster(conf: HamaConfiguration) extends Master(conf) {

  override def initializeServices {
    create("receptionist", classOf[MockReceptionist])
  }
  
}

class MockReceptionist(conf: HamaConfiguration) extends Receptionist(conf) {

  override val LOG = Logging(context.system, this)

  def getJob: Receive = {
    case GetJob(tester) => {
      if(waitQueue.isEmpty) 
        throw new NullPointerException("Job is not enqueued for waitQueue is "+
                                       "empty!");
      val job = waitQueue.dequeue._1
      LOG.info("GetJob: job -> {}", job)
      tester ! JobContent(job.getId, 
                          job.getLocalJarFile
                          /*, job.getLocalJobFile*/)
    }
  }
  
  override def receive = getJob orElse super.receive

}

@RunWith(classOf[JUnitRunner])
class TestReceptionist extends TestEnv("TestReceptionist") with JobUtil {

  var receptionist: ActorRef = _

  it("test submit job to receptionist") {
    LOG.info("Test submit job to Receptionist")
    val master = create("bspmaster", classOf[MockMaster])
    val jobId = createJobId("test-receptionist", 1533)
    // set target GroomServers
    testConfiguration.setStrings("bsp.sched.targets.grooms", 
                                 "groom5", "groom4", "groom5", "groom4", 
                                 "groom1") 
    val jobFilePath = createJobFile(testConfiguration)
    LOG.info("Submit job id "+jobId.toString+" job.xml: "+jobFilePath)
    master ! Request("receptionist", GroomStat("groom1", 3))
    master ! Request("receptionist", GroomStat("groom4", 2))
    master ! Request("receptionist", GroomStat("groom5", 7))
    LOG.info("Wait 1 sec ...")
    sleep(1.seconds)
    master ! Request("receptionist", Submit(jobId, jobFilePath))
    LOG.info("Wait 5 secs ...")
    sleep(5.seconds)
    master ! Request("receptionist", GetJob(tester))
    expect(
      JobContent(jobId, 
                 "/tmp/local/bspmaster/job_test-receptionist_1533.jar"/*, 
                 "/tmp/local/bspmaster/job_test-receptionist_1533.xml"*/)
    )
  }
}
