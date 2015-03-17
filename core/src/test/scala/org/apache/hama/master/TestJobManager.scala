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
import org.apache.hama.MockClient
import org.apache.hama.TestEnv
import org.apache.hama.bsp.v2.Job
import org.apache.hama.util.JobUtil
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TestJobManager extends TestEnv("TestJobManager") with JobUtil {

  it("test job management functions.") {
    val job = createJob("test", 2, "test-job-manager", 3)
    val jobManager = JobManager.create
    val readyToPull = jobManager.readyForNext
    LOG.info("Ready to pull next job? {}", readyToPull)
    assert(readyToPull)
    jobManager.enqueue(Ticket(client, job)) 

    jobManager.ticketAt match {
      case (s: Some[Stage], t: Some[Ticket]) => {
        val stage = s.get
        val ticket = t.get
        LOG.info("Ticket {} found stage {}!", ticket, stage)
        assert(stage.equals(TaskAssign))
        val jobId = ticket.job.getId
        LOG.info("Ticket's job id is {}", jobId.toString)
        assert("job_test_0002".equals(jobId.toString))
        LOG.info("Initial job state is {}", ticket.job.getState)
        assert(ticket.job.getState.equals(Job.State.PREP))
      }
      case _ => throw new RuntimeException("Ticket not found!")
    }
    val jobFound = jobManager.headOf(TaskAssign).
                              map { ticket => ticket.job }.
                              getOrElse(null)
    LOG.info("Job found in manager: {}", jobFound)
    assert(job.equals(jobFound))

    jobManager.findJobById(job.getId) match {
      case (s: Some[Stage], j: Some[Job]) => {
        LOG.info("Find a job {} by id {} at stage {}", j.get, job.getId, s.get)
        assert(job.equals(j.get))
      }
      case _ => throw new RuntimeException("Job not found!")
    }

    val moveToProcessing = jobManager.move(job.getId)(Processing)
    LOG.info("Successfully move to processing stage? {}", moveToProcessing)
    assert(moveToProcessing)
    
    val emptyTaskAssign = jobManager.headOf(TaskAssign).getOrElse(null)
    assert(null == emptyTaskAssign)

    jobManager.ticketAt match {
      case (s: Some[Stage], t: Some[Ticket]) => {
        val stage = s.get
        val ticket = t.get
        LOG.info("Ticket now is moved to stage {}", stage)
        assert(Processing.equals(stage))
        val result = jobManager.update(ticket.newWith(ticket.
                     job.newWithFailedState))
        LOG.info("Successfully update job to fail state? {}", result)
        assert(result) 
      }
      case _ => throw new RuntimeException("Ticket not found!")
    }

    jobManager.findJobById(job.getId) match {
      case (s: Some[Stage], j: Some[Job]) => {
        val newStage = s.get
        val newJob = j.get
        LOG.info("Job is now at stgae {}", newStage)
        assert(Processing.equals(newStage))
        val newState = newJob.getState
        LOG.info("Job is now at state {}", newState)
        assert(Job.State.FAILED.equals(newState))
      }
      case _ => throw new RuntimeException("Job not found!")
    }

    jobManager.moveToNextStage(job.getId) match {
      case (true, s: Option[Stage]) => {
        val stage = s.get
        LOG.info("New stage should be Finished: {}", stage)
        assert(Finished.equals(stage))
      }
      case _ => throw new RuntimeException("Fail moving the job to next stage!")
    }

    val (b, none) = jobManager.moveToNextStage(job.getId)
    LOG.info("Moving the job in Finsihed stage again should return "+
             "true and None! Actual result: {} {}!", b, none)
    assert(b == true && None.equals(none))

    LOG.info("Done testing JobManager functions!")    
  }
}
