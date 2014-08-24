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
import org.apache.hama.TestEnv
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

private final case class GetStat(groomServerName: String, from: ActorRef)
private final case class Stat(groomServerName: String, maxTasks: Int)
private final case class GetEnrollment(groomServerName: String, 
                                       tester: ActorRef)
private final case class EnrollmentStat(groomName: String, maxTasks: Int)
private final case class GetMaxTasksSum(from: ActorRef)
private final case class MaxTasksSum(sum: Int)

class MockRecep(conf: HamaConfiguration) extends Receptionist(conf) {

  def getMaxTasksSum: Receive = {
    case GetMaxTasksSum(from) => {
      from ! MaxTasksSum(maxTasksSum)
    }
  }
  override def receive = getMaxTasksSum orElse super.receive
}

class MockSched(conf: HamaConfiguration, receptionist: ActorRef) 
      extends Scheduler(conf, receptionist) { 

  def getEnrollment: Receive = {
    case GetEnrollment(groomName, from) => { 
      LOG.info("groom task managers: {} ", groomTaskManagers.mkString(", "))
      groomTaskManagers.get(groomName) match { 
        case Some(value) => from ! EnrollmentStat(groomName, value._2)
        case None => throw new RuntimeException("GroomServer "+groomName+
                                                " stat not found.")
      }
    }
  }

  override def nextPlease: Receive = {
    case NextPlease => { 
      LOG.info("Disable pulling a job from waitQueue!")
    }
  }

  override def receive = getEnrollment orElse super.receive
}

class MockGroomManager(conf: HamaConfiguration, receptionist: ActorRef,
                       sched: ActorRef) 
      extends GroomManager(conf, receptionist, sched) {

  def getStat: Receive = {
    case GetStat(groomServerName, from) => {
      if(null == groomServerName || groomServerName.isEmpty)
        throw new IllegalArgumentException("Invalide groomServerName!") 
      grooms.find(p=>p.groomServerName.equals(groomServerName)) match {
         case Some(found) => from ! Stat(found.groomServerName, 
                                         found.maxTasks)
         case None => 
           throw new RuntimeException("GroomServer %s not registered!".
                                      format(groomServerName))
      } 
    }
  }
  
  override def receive = getStat orElse super.receive

}

@RunWith(classOf[JUnitRunner])
class TestGroomManager extends TestEnv("TestGroomManager") {

  it("test enroll groom servers to groom manager.") {
    LOG.info("Test GroomManager logic...")
    val receptionist = createWithArgs("receptionist", classOf[MockRecep], 
                                      testConfiguration)
    val sched = createWithArgs("sched", classOf[MockSched], testConfiguration, 
                               receptionist)
    val groomManager = createWithArgs("groomManager", classOf[MockGroomManager],
                                      testConfiguration, receptionist, sched)
    //sleep(3.seconds)
    val reg1 = new Register("groom7", 3) 
    val reg2 = new Register("groom4", 2) 
    val reg3 = new Register("groom9", 7) 
    groomManager ! reg1
    groomManager ! reg2
    groomManager ! reg3
    //sleep(5.seconds)
    groomManager ! GetStat("groom7", tester)
    groomManager ! GetStat("groom4", tester)
    groomManager ! GetStat("groom9", tester)
    LOG.info("Verifying GroomManager's groom server maxTasks value ...")
    expect(Stat("groom7", 3)) 
    expect(Stat("groom4", 2)) 
    expect(Stat("groom9", 7)) 
    //sleep(5.seconds)
    sched ! GetEnrollment("groom7", tester)
    sched ! GetEnrollment("groom4", tester)
    sched ! GetEnrollment("groom9", tester)
    LOG.info("Verifying Scheduler's groom server maxTasks value ...")
    expect(EnrollmentStat("groom7", 3))  
    expect(EnrollmentStat("groom4", 2))  
    expect(EnrollmentStat("groom9", 7))  
    receptionist ! GetMaxTasksSum(tester)
    //sleep(3.seconds)
    LOG.info("Verifying Receptionist's groom server maxTasksSum value ...")
    expectAnyOf(MaxTasksSum(3), MaxTasksSum(5), MaxTasksSum(12))
  }
}
