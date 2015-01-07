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
import akka.actor.Props
import org.apache.hama.Agent
import org.apache.hama.Periodically
import org.apache.hama.TestEnv
import org.apache.hama.Tick
import org.apache.hama.bsp.v2.Job
import org.apache.hama.conf.Setting
import org.apache.hama.groom.TaskRequest
import org.apache.hama.groom.RequestTask
import org.apache.hama.monitor.GroomStats
import org.apache.hama.util.JobUtil
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

class MockF extends Agent {
  override def receive = unknown
}

// client
class MockC extends Agent {
  override def receive = unknown
}

// receptionist
class MockR(client: ActorRef, job: Job) extends Agent {

  override def receive = {
    case TakeFromWaitQueue => {
      LOG.info("Mock receptionist dispenses a job having id {}", job.getId)
      sender ! Dispense(Ticket(client, job))
    }
  }

}

// groom passive
class MockGP(id: Int, sched: ActorRef, tester: ActorRef) 
      extends Agent {

  def currentGroomStats(): GroomStats = {
    val name = "groom-passive" + id
    val host = "groom" + id
    val port = 50000 + id
    val maxTasks = 3
    LOG.info("Create mock groom stats for groom id {} with name {}, "+
             "host {}, port {}, maxTasks {}, ", id, name, host, port, maxTasks)
    GroomStats(name, host, port, maxTasks)
  }

  def request: Receive = {
    case TaskRequest => {
      val stats = currentGroomStats
      LOG.info("Groom {} requests for task assign", id, stats)
      sched ! RequestTask(stats)
    }
  }

  def receiveDirective: Receive = {
    case directive: Directive => {
      val action = directive.action.toString
      val taskId = directive.task.getId.toString
      val state = directive.task.getState.toString
      val phase = directive.task.getPhase.toString
      val isActive = directive.task.isActive.toString
      val isAssigned = directive.task.isAssigned.toString
      val host = directive.task.getAssignedHost.toString
      val port = directive.task.getAssignedPort.toString
      val totalTasks = directive.task.getTotalBSPTasks.toString
      val d = Seq(action, taskId, state, phase, isActive, isAssigned, host, 
                  port, totalTasks)
      LOG.info("(passive mode) mock groom {} receives directive {}", name, d)
      tester ! d 
    }
  }

  override def receive = receiveDirective orElse request orElse unknown 

}

// groom active
class MockGA(tester: ActorRef) extends Agent {

  override def receive = {
    case directive: Directive => {
      val action = directive.action.toString
      val taskId = directive.task.getId.toString
      val state = directive.task.getState.toString
      val phase = directive.task.getPhase.toString
      val isActive = directive.task.isActive.toString
      val isAssigned = directive.task.isAssigned.toString
      val host = directive.task.getAssignedHost.toString
      val port = directive.task.getAssignedPort.toString
      val totalTasks = directive.task.getTotalBSPTasks.toString
      val d = Seq(action, taskId, state, phase, isActive, isAssigned, host, 
                  port, totalTasks)
      LOG.info("(active mode) mock groom {} receives directive {}", name, d)
      tester ! d 
    }
  }
}

// master
class MockM(tester: ActorRef) extends Agent {

  override def receive = {
    case GetTargetRefs(targetGrooms) => {
      LOG.info("Who sends GetTargetRefs: {} ", sender)
      var refs = Array.empty[ActorRef]
      for(idx <- 0 until targetGrooms.size) {
        refs ++= Array(context.system.actorOf(Props(classOf[MockGA], tester), 
                                            "mockGA"+idx))
      }
      sender ! TargetRefs(refs)    
    }
  }

}

class MockScheduler(setting: Setting, master: ActorRef, 
                    receptionist: ActorRef, federator: ActorRef, 
                    tester: ActorRef) 
      extends Scheduler(setting, master, receptionist, federator) {

  var isActive = false

  val hosts = Array("groom214", "groom129", "groom9")
  val ports = Array(51144, 50002, 58249)
  var aidx = 0

  val passiveHosts = Array("groom1", "groom2")
  val passivePorts = Array(50001, 50002)
  var pidx = 0

  override def targetRefsFound(refs: Array[ActorRef]) {
    isActive = true
    super.targetRefsFound(refs)
    isActive = false
  }
  
  override def getTargetHostPort(ref: ActorRef): (String, Int) = 
    if(isActive) {
      val host = hosts(aidx) 
      val port = ports(aidx) 
      aidx += 1
      (host, port)
    } else {
      val host = passiveHosts(pidx) 
      val port = passivePorts(pidx) 
      pidx += 1
      (host, port)
    }
}

@RunWith(classOf[JUnitRunner])
class TestScheduler extends TestEnv("TestScheduler") with JobUtil {

  val d1 = Seq("Launch", "attempt_test_0003_000001_1", "WAITING", "SETUP",
               "true", "true", "groom214", "51144", "5")
  val d2 = Seq("Launch", "attempt_test_0003_000002_1", "WAITING", "SETUP",
               "true", "true", "groom129", "50002", "5")
  val d3 = Seq("Launch", "attempt_test_0003_000003_1", "WAITING", "SETUP",
               "true", "true", "groom9", "58249", "5")
  val d4 = Seq("Launch", "attempt_test_0003_000004_1", "WAITING", "SETUP",
               "false", "true", "groom1", "50001", "5")
  val d5 = Seq("Launch", "attempt_test_0003_000005_1", "WAITING", "SETUP",
               "false", "true", "groom2", "50002", "5")


  it("test scheduling tasks") {
    val setting = Setting.master
    val targets = Array("groom214:51144", "groom129:50002", "groom9:58249")
    val job = createJob("test", 3, "sched-active-passive", targets, 5)
    val c = createWithArgs("mockClient", classOf[MockC])
    val f = createWithArgs("mockFederator", classOf[MockF])
    val m = createWithArgs("mockMaster", classOf[MockM], tester) 
    val r = createWithArgs("mockReceptionist", classOf[MockR], c, job) 
    val sched = createWithArgs(Scheduler.simpleName(setting.hama), 
                               classOf[MockScheduler], setting, m, r, f,
                               tester)  

    expectAnyOf(d1, d2, d3)
    expectAnyOf(d1, d2, d3)
    expectAnyOf(d1, d2, d3)

    val gp1 = createWithArgs("mockGP1", classOf[MockGP], 1, sched, tester) 
    val gp2 = createWithArgs("mockGP2", classOf[MockGP], 2, sched, tester) 
   
    gp1 ! TaskRequest
    gp2 ! TaskRequest

    expectAnyOf(d4, d5)
    expectAnyOf(d4, d5)
  }
}
