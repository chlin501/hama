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
import org.apache.hama.Mock
import org.apache.hama.Periodically
import org.apache.hama.TestEnv
import org.apache.hama.Tick
import org.apache.hama.bsp.v2.Job
import org.apache.hama.conf.Setting
import org.apache.hama.master.Directive.Action._
import org.apache.hama.groom.TaskRequest
import org.apache.hama.groom.RequestTask
import org.apache.hama.monitor.GroomStats
import org.apache.hama.util.JobUtil
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

trait MockTC extends Mock with Periodically {// task counsellor

  def testMsg: Receive = {
    case directive: Directive => directive match {
      case null => throw new RuntimeException("Directive shouldn't be null!")
      case d@_ => received(d)
    }
  }

  override def ticked(msg: Tick): Unit = msg match {
    case TaskRequest => requestNewTask()
  }

  def received(d: Directive) { }

  def requestNewTask() { }

  override def receive = testMsg orElse tickMessage orElse super.receive
}

class Passive1(sched: ActorRef, tester: ActorRef) extends MockTC {

   override def received(d: Directive) = d.action match { 
     case Launch =>
     case Resume =>
     case Kill =>
   }
}

class Passive2(sched: ActorRef, tester: ActorRef) extends MockTC {

   override def received(d: Directive) = d.action match { 
     case Launch =>
     case Resume =>
     case Kill =>
   }
}

class Passive3(sched: ActorRef, tester: ActorRef) extends MockTC {

   override def received(d: Directive) = d.action match { 
     case Launch =>
     case Resume =>
     case Kill =>
   }
}

class Passive4(sched: ActorRef, tester: ActorRef) extends MockTC {

   override def received(d: Directive) = d.action match { 
     case Launch =>
     case Resume =>
     case Kill =>
   }
}

class Passive5(sched: ActorRef, tester: ActorRef) extends MockTC {

   override def received(d: Directive) = d.action match { 
     case Launch =>
     case Resume =>
     case Kill =>
   }
}

class Active1(sched: ActorRef, tester: ActorRef) extends MockTC {

   override def received(d: Directive) = d.action match { 
     case Launch =>
     case Resume =>
     case Kill =>
   }
}

class Active2(sched: ActorRef, tester: ActorRef) extends MockTC {

   override def received(d: Directive) = d.action match { 
     case Launch =>
     case Resume =>
     case Kill =>
   }
}

class Active3(sched: ActorRef, tester: ActorRef) extends MockTC {

   override def received(d: Directive) = d.action match { 
     case Launch =>
     case Resume =>
     case Kill =>
   }
}

class Client(tester: ActorRef) extends Mock

class Master(actives: Array[ActorRef]) extends Mock {

  def testMsg: Receive = {
    case GetTargetRefs(infos) => {
      LOG.info("Active grooms {} found!", actives.mkString(", "))
      sender ! TargetRefs(actives)
    }
  }

  override def receive = testMsg orElse super.receive
}

class F extends Mock

final case class J(job: Job)
// receptionist
class R(client: ActorRef) extends Mock {

  var job: Option[Job] = None

  def testMsg: Receive = {
    case J(j) => job = Option(j)
    case TakeFromWaitQueue => job match {
      case None => throw new RuntimeException("Job is not yet ready!")
      case Some(j) => {
        LOG.info("Job {} is sent to scheduler!", j.getId)
        sender ! Dispense(Ticket(client, j))
      }
    }
  }

  override def receive = testMsg orElse super.receive
}

class MockScheduler(setting: Setting, master: ActorRef, receptionist: ActorRef,
                    federator: ActorRef, tester: ActorRef) 
      extends Scheduler(setting, master, receptionist, federator) {

   

}

@RunWith(classOf[JUnitRunner])
class TestScheduler extends TestEnv("TestScheduler") with JobUtil {

  val actives: Array[ActorRef] = Array(
    createWithArgs("active1", classOf[Active1], tester),
    createWithArgs("active2", classOf[Active2], tester), 
    createWithArgs("active1", classOf[Active1], tester)
  )

  val passives: Array[ActorRef] = Array(
    createWithArgs("passive1", classOf[Passive1], tester),
    createWithArgs("passive2", classOf[Passive2], tester), 
    createWithArgs("passive3", classOf[Passive3], tester),
    createWithArgs("passive4", classOf[Passive4], tester),
    createWithArgs("passive5", classOf[Passive5], tester)
  )

  def jobWithActiveGrooms(ident: String, id: Int): Job = createJob(ident, id, 
    "test-sched", Array("host123:412", "host1:1924", "host717:22123"), 8)

  it("test scheduling functions.") {
    val setting = Setting.master
    val job = jobWithActiveGrooms("test", 2)
    val client = createWithArgs("mockClient", classOf[Client], tester)
    val receptionist = createWithArgs("mockReceptionist", classOf[R], client)
    receptionist ! J(job)
    val federator = createWithArgs("mockFederator", classOf[F])
    val master = createWithArgs("mockMaster", classOf[Master], actives)
    val sched = createWithArgs("mockSched", classOf[MockScheduler], master, 
                               receptionist, federator, tester)
    
    LOG.info("Done testing scheduler functions!")
  }


}
