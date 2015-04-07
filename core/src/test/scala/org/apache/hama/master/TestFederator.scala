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

import akka.actor.Actor
import akka.actor.ActorRef
import org.apache.hama.HamaConfiguration
import org.apache.hama.TestEnv
import org.apache.hama.MockClient
import org.apache.hama.bsp.BSPJobID
import org.apache.hama.conf.Setting
import org.apache.hama.logging.ActorLog
import org.apache.hama.monitor.ListService
import org.apache.hama.monitor.master.TotalMaxTasks
import org.apache.hama.util.JobUtil
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

final case class DoValidate(v: Validate)
final case object GetValidation
final case class ValidationSize(v: Int)

class MockFederator(setting: Setting, master: ActorRef, tester: ActorRef) 
      extends Federator(setting, master) {

  override def currentTrackers(): Array[String] = {
    val trackers = super.currentTrackers
    tester ! trackers.sorted.toSeq
    trackers
  }

  override def allVerified(jobId: BSPJobID): Option[Validate] = {
    val r = super.allVerified(jobId)
    r match {
      case Some(v) => {
        LOG.info("All validated for job id {}!", jobId)
        tester ! v.validated.jobId
        v.validated.actions.foreach { case (k, v) => tester ! (k, v) }
      }
      case None => LOG.info("Job id {} has validated {}", jobId, 
                            findValidateBy(jobId).actions)
    }
    r
  }

/* TODO: this func is replaced by service event listener.
  override def inform(service: String, result: ProbeMessage) = {
    if(result.toString.contains("TotalMaxTasks")) {
      val r = result.asInstanceOf[TotalMaxTasks]
      LOG.info("Rewrite service {} result {} tasks to 1024 ..", service, result)
      self ! TotalMaxTasks(r.jobId, 1024)
    } else super.inform(service, result)

  }
*/

  def getValidation: Receive = {
    case GetValidation => {
      LOG.info("Validation size now is {}", validation.size)
      tester ! ValidationSize(validation.size)
    }
  }

  override def receive = getValidation orElse super.receive
}


class MockMaster(setting: Setting, tester: ActorRef) 
      extends BSPMaster(setting, "test-identifier") {

  override def initializeServices {
    getOrCreate(Federator.simpleName(setting), classOf[MockFederator], setting, 
                self, tester) 
  }

  def listTracker: Receive = {
    case ListTracker => 
      findServiceBy(Federator.simpleName(setting)).map { fed => 
        fed ! ListTracker
      }
  }

  override def listServices(from: ActorRef) = from.path.name match {
    case "deadLetters" => {
      val available = services.toArray
      LOG.info("{} requests current master services available: {}", 
               from.path.name, available.mkString(","))
      tester ! available.sorted.toSeq
    }
    case _ => LOG.info("Others request for master services available {}", 
                       from.path.name)
  }

  def doValidate: Receive = {
    case DoValidate(validate) => 
      findServiceBy(Federator.simpleName(setting)).map { fed =>
        fed ! validate
      }
  }

  val hosts = Array("groom21", "groom13", "groom4122")
  val ports = Array(51144, 50014, 50021)

  override def groomsExist(host: String, port: Int): Boolean = 
    if(hosts.contains(host) && ports.contains(port)) true else false

  def getValidation: Receive = {
    case GetValidation => 
      findServiceBy(Federator.simpleName(setting)).map { fed => {
        LOG.info("Forward GetValidation to federator ...")
        fed forward GetValidation
      }}
  }
 
  override def receive = getValidation orElse doValidate orElse listTracker orElse super.receive 

}

@RunWith(classOf[JUnitRunner])
class TestFederator extends TestEnv("TestFederator") with JobUtil {

  val masterSetting = {
    val setting = Setting.master
    setting.hama.set("master.name", classOf[MockMaster].getSimpleName)
    setting.hama.set("master.main", classOf[MockMaster].getName)
    setting
  }

  val jobConf = {
    val conf = new HamaConfiguration
    conf.setStrings("bsp.target.grooms", "groom21:51144", 
                                         "groom13:50014", 
                                         "groom4122:50021")
    conf.setInt("bsp.peers.num", 5)
    conf 
  }

  it("test federator functions.") {
    val expectedServices = Seq(Federator.simpleName(masterSetting))
    val receptionist = client // TODO: change receptionist to real one if needed
    val master = createWithArgs(masterSetting.name, masterSetting.main, 
                               masterSetting, tester)
        
    master ! ListService
    expect(expectedServices)
 
    master ! ListTracker 
    expect(Federator.defaultTrackers.sorted.toSeq)

    val jobId = createJobId("test", 3)
    val v1 = Validate(jobId, jobConf, client, receptionist,  
                      Map(CheckMaxTasksAllowed -> NotVerified, 
                          IfTargetGroomsExist -> NotVerified))
    master ! DoValidate(v1)
    expect(jobId)
    expect((CheckMaxTasksAllowed, Valid))
    expect((IfTargetGroomsExist, Valid))

    master ! GetValidation
    expect(ValidationSize(0))
  }
}
