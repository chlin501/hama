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
import org.apache.hama.HamaConfiguration
import org.apache.hama.TestEnv
import org.apache.hama.bsp.BSPJobID
import org.apache.hama.bsp.v2.Job
import org.apache.hama.conf.Setting
import org.apache.hama.io.PartitionedSplit
import org.apache.hama.util.JobUtil
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

private class MockMaster1(setting: Setting) 
      extends BSPMaster(setting, null.asInstanceOf[Registrator])

class MockFed(setting: Setting, master: ActorRef) 
      extends Federator(setting, master) {

  override def validate: Receive = {
    case conditions: Validate => {
      var newActions = Map.empty[Any, Validation] 
      conditions.actions.foreach { case (k, v) => 
        newActions ++= Map(k -> Valid) 
      }
      LOG.info("Update all validation to valid: {}", newActions)
      val v1 = Validate(conditions.jobId, conditions.jobConf, 
                        conditions.client, conditions.receptionist, 
                        newActions)
      LOG.info("Send validated result back to {}", sender.path.name)
      sender ! v1.validated
    }
  }
}

class MockReceptionist(setting: Setting, federator: ActorRef, tester: ActorRef) 
      extends Receptionist(setting, federator) {

  override def enqueueJob(newJob: Job) = {
    LOG.info("Job created is {}", newJob)
    tester ! newJob.getId.toString
    val targetGrooms = newJob.getConfiguration.getStrings("bsp.targets.grooms")
    LOG.info("Target grooms include {}", targetGrooms.mkString(","))
    tester ! targetGrooms.mkString(",")
  }

}

@RunWith(classOf[JUnitRunner])
class TestReceptionist extends TestEnv("TestReceptionist") with JobUtil {

  val jobConf = {
    val conf = new HamaConfiguration
    conf.setStrings("bsp.targets.grooms", "groom5:50021", "groom4:51144", 
                                          "groom5:50021", "groom4:51144", 
                                          "groom1:50002") 
    conf
  }

  it("test submit job to receptionist") {
    val setting = Setting.master
    val master = createWithArgs(setting.name, classOf[MockMaster1], setting)
    val fed = createWithArgs(Federator.simpleName(setting.hama),
                             classOf[MockFed], setting, master)
    val receptionist = createWithArgs(Receptionist.simpleName(setting.hama), 
                                      classOf[MockReceptionist], setting, fed,
                                      tester)
    val jobId = createJobId("test-receptionist", 1533)
    val jobFilePath = createJobFile(jobConf)
    LOG.info("Submit job id "+jobId.toString+" job.xml: "+jobFilePath)
    receptionist ! Submit(jobId, jobFilePath)
    sleep(6.seconds)
    expect(jobId.toString) 
    expect(jobConf.getStrings("bsp.targets.grooms").mkString(","))
  }
}
