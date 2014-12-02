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
import org.apache.hama.HamaConfiguration
import org.apache.hama.TestEnv
import org.apache.hama.groom._
import org.apache.hama.conf.Setting
import org.apache.hama.io.PartitionedSplit
import org.apache.hama.util.JobUtil
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

private final case class GetJob(tester: ActorRef)
private final case class JobContent(jobId: BSPJobID, 
                                    localJarFile: String)

class MockReceptionist(setting: Setting) 
      extends Receptionist(setting, null.asInstanceOf[ActorRef]) {

  def getJob: Receive = {
    case GetJob(tester) => {
      if(waitQueue.isEmpty) 
        throw new NullPointerException("Job is not enqueued for waitQueue is "+
                                       "empty!");
      val job = waitQueue.dequeue._1
      LOG.info("GetJob: job -> {}", job)
      job.getLocalJarFile match {
        case null => tester ! JobContent(job.getId, null)
        case _ => throw new Error("Jar file path is created by BSPJobClient."+
                                   "So this should be empty!")
      }
    }
  }
  
  override def receive = getJob orElse super.receive

}

@RunWith(classOf[JUnitRunner])
class TestReceptionist extends TestEnv("TestReceptionist") with JobUtil {

  var receptionist: ActorRef = _

  it("test submit job to receptionist") {
    LOG.info("Test submit job to Receptionist")
    val receptionist = create("receptionist", classOf[MockReceptionist])
    val jobId = createJobId("test-receptionist", 1533)
    // set target GroomServers
    testConfiguration.setStrings("bsp.sched.targets.grooms", 
                                 "groom5", "groom4", "groom5", "groom4", 
                                 "groom1") 
    val jobFilePath = createJobFile(testConfiguration)
    LOG.info("Submit job id "+jobId.toString+" job.xml: "+jobFilePath)
    receptionist ! GroomStat("groom1", 3)
    receptionist ! GroomStat("groom4", 2)
    receptionist ! GroomStat("groom5", 7)
    receptionist ! Submit(jobId, jobFilePath)
    receptionist ! GetJob(tester)
    // N.B.: the second parameter is "bsp.jar" and it should be null because
    //       jar path is originally configured by BSPJobClient.
    expect(JobContent(jobId, null)) 
  }
}
