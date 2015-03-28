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

@RunWith(classOf[JUnitRunner])
class TestAssigner extends TestEnv("TestAssigner") with JobUtil {

  import GroomStats._

  val host1 = "groom1"
  val host2 = "groom2"
  val port1 = 41231
  val port2 = 21941

  def stats(host: String, port: Int): GroomStats = 
    GroomStats(host, port, defaultMaxTasks)

  def mock(name: String): ActorRef = createWithArgs(name, classOf[Mock])

  it("test task assign functions.") {
    val jobManager = JobManager.create
    val job = createJob("test", 3, "assigner-job", 2)
    val ticket = Ticket(client, job)
    jobManager.enqueue(ticket) 
    val assigner = Assigner.create(new HamaConfiguration, jobManager)
    val none = assigner.examine(jobManager)
    assert(None.equals(none))
    jobManager.markScheduleFinished
    val some = assigner.examine(jobManager)
    assert(!None.equals(some))
    assert(3 == some.get.job.getId.getId)
    assert("assigner-job".equals(some.get.job.getName))

    doAssign(assigner, jobManager, ticket, "groom1", host1, port1)
    jobManager.findJobById(job.getId) match {
      case (s: Some[Stage], t: Some[Job]) => assert(TaskAssign.equals(s.get))
      case _ => throw new RuntimeException("Invalid stage for job "+job.getId)
    }

    doAssign(assigner, jobManager, ticket, "groom2", host2, port2)
    jobManager.findJobById(job.getId) match {
      case (s: Some[Stage], t: Some[Job]) => assert(Processing.equals(s.get))
      case _ => throw new RuntimeException("Invalid stage for job "+job.getId)
    }

    LOG.info("Done testing Assigner functions!")    
  }

  def doAssign(assigner: Assigner, jobManager: JobManager, ticket: Ticket, 
                 groom: String, host: String, port: Int) {
    val validated = assigner.validate(ticket, stats(host, port))
    assert(true == validated)
    assigner.assign(ticket, stats(host, port), mock(groom))
  }
}