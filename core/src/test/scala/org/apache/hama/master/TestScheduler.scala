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

import akka.actor._
import akka.event._
import akka.testkit._
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import org.apache.hama._
import org.apache.hama.bsp.BSPJobID
import org.apache.hama.bsp.v2._
import org.apache.hama.bsp.v2.IDCreator._
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

case class JobData(fromQueueSize: Int, toQueueSize: Int, jobId: BSPJobID)
case object GetJobData
case object GetTarget
case object AddProxy
case object ProxyAdded

class MockTaskManager extends Actor {
  override def receive = {
    case _ =>  
  }
}

class MockSched(conf: HamaConfiguration, ref: ActorRef) 
    extends Scheduler(conf) {

  override val LOG = Logging(context.system, this)

  var task: Task = _

  override def dispatch(from: ActorRef, task: Task) {
    LOG.info("Task ({}) will be dispatch to {}", 
             task.getAssignedTarget, from.path.name)
    this.task = task
  }
  
  def addMockProxy: Receive = {
    case AddProxy => {
      LOG.info("Who is the sender? {}", ref.path.name)
      LOG.info("Before looking up MockTaskManager, proxies size  {}", 
               proxies.size)
      proxies ++= 
        Set(context.system.actorOf(Props(classOf[MockTaskManager]), "groom1"))
      if(1 != proxies.size)
        throw new RuntimeException("Proxy size should be 1!");
      LOG.info("Proxies size now is {}!", proxies.size)
      ref ! ProxyAdded
    }
  }
 
  def getTask: Receive = {
    case GetTarget => {
      ref ! task.getAssignedTarget
    }
  }

  def getJobData: Receive = {
    case GetJobData => {
      val fromQueueSize = taskAssignQueue.size
      val toQueueSize = processingQueue.size
      val (job, rest) = processingQueue.dequeue
      LOG.info("TaskAssignQueue size: {}, ProcessingQueue has size {}, "+
               " and the job id is {}", 
               fromQueueSize, toQueueSize, job.getId)
      ref ! JobData(fromQueueSize, toQueueSize, job.getId)
    }
  }
  
  override def receive = getJobData orElse addMockProxy orElse getTask orElse super.receive
}

@RunWith(classOf[JUnitRunner])
class TestScheduler extends TestKit(ActorSystem("TestScheduler")) 
                                    with FunSpecLike 
                                    with ShouldMatchers 
                                    with BeforeAndAfterAll {

  val LOG = LogFactory.getLog(classOf[TestScheduler])

  override protected def afterAll {
    system.shutdown
  }

  def createJob(): Job = {
    val jobId = IDCreator.newBSPJobID.withId("test").withId(7).build
    new Job.Builder().setId(jobId).
                      setName("test-scheduler").
                      withTarget("groom1").
                      withTaskTable.
                      build
  }

  it("test scheduler") {
      val prob = TestProbe()
      val conf = new HamaConfiguration
      val sched = system.actorOf(Props(classOf[MockSched], conf, prob.ref))
      sched ! AddProxy
      prob.expectMsg(ProxyAdded)
      sched ! Dispense(createJob)
      sched ! GetTarget
      prob.expectMsg("groom1")
      sched ! GetJobData
      prob.expectMsg(JobData(0, 1, createJob.getId))
  }
}
