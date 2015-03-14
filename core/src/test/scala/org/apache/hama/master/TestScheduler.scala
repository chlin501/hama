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
import org.apache.hama.Mock
import org.apache.hama.TestEnv
import org.apache.hama.bsp.v2.Job
import org.apache.hama.monitor.GroomStats
import org.apache.hama.util.JobUtil
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

class MockMaster2(tester: ActorRef) extends Mock {

  def msgs: Receive = {
    case GetTargetRefs(ary) => tester ! ary.map { e => 
      e.getHost+":"+e.getPort 
    }.mkString(",")
  }

  override def receive = msgs orElse super.receive 
}

@RunWith(classOf[JUnitRunner])
class TestScheduler extends TestEnv("TestScheduler") with JobUtil {

  import GroomStats._

  val host1 = "groom1"
  val host2 = "groom2"
  val port1 = 41231
  val port2 = 21941
  val targetGrooms = Array[String](host1+":"+port1, host2+":"+port2)

  def stats(host: String, port: Int): GroomStats = 
    GroomStats(host, port, defaultMaxTasks)

  it("test task scheduling functions.") {
    val jobManager = JobManager()
    val job = createJob("test", 3, "test-job-sched", targetGrooms, 2)
    val ticket = Ticket(client, job)
    val scheduler = Scheduler.create(new HamaConfiguration, jobManager)
    scheduler.receive(ticket)
    assert(false == jobManager.allowPassiveAssign)
    val hasTargetGrooms = scheduler.examine(ticket)
    assert(true == hasTargetGrooms)
    val master = createWithArgs("master", classOf[MockMaster2], tester)
    scheduler.findGroomsFor(ticket, master)
    expect(targetGrooms.mkString(","))

    LOG.info("Done testing Scheduler functions!")    
  }

}
