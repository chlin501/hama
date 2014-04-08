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
import org.apache.hama.groom._
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

class MockTaskManager(conf: HamaConfiguration, ref: ActorRef, 
                      mockSched: ActorRef) extends TaskManager(conf) {

  override val LOG = Logging(context.system, this)

  /** 
   * Disable remote lookup sched.
   * Replace sched with mock sched.
   */
  override def initializeServices = {
    initializeSlots
    sched = mockSched
    LOG.info("Schedule request TaskRequest message.")
    request(self, TaskRequest)
  }

  override def afterLinked(proxy: ActorRef) { }
  
  override def receive = super.receive
}

class MockSched(conf: HamaConfiguration, actorName: String, ref: ActorRef) 
    extends Scheduler(conf) {

  override val LOG = Logging(context.system, this)

  var task: Task = _

  override def dispatch(from: ActorRef, task: Task) {
    LOG.info("Task ({}) will be dispatch to {}", 
             task.getAssignedTarget, from.path.name)
    this.task = task
    LOG.info("ActorName: {}, target groom name: {}", 
             actorName, task.getAssignedTarget)
    if("groom_127.0.0.1_50000".equals(actorName)) {
      LOG.info("Task is dispatched to the groom that perfoms request.")
      ref ! task.getAssignedTarget
    }
  }
  
  def addMockProxy: Receive = {
    case AddProxy => {
      LOG.info("Who is the sender? {}", ref.path.name)
      LOG.info("Before looking up MockTaskManager, proxies size  {}", 
               proxies.size)
      LOG.info("TaskManager name: {}", actorName)
      proxies ++= 
        Set(context.system.actorOf(Props(classOf[MockTaskManager], 
                                         conf, 
                                         ref, 
                                         self), 
                                   actorName))
      if(1 != proxies.size)
        throw new RuntimeException("Proxy size should be 1!");
      LOG.info("Proxies size now is {}!", proxies.size)
      ref ! ProxyAdded
    }
  }
 
  def getTask: Receive = {
    case GetTarget => ref ! task.getAssignedTarget
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
  val prob = TestProbe()
  val conf = new HamaConfiguration
  var sched: ActorRef = _

  override protected def afterAll {
    system.shutdown
  }

  def createActiveJob(): Job = {
    val jobId = IDCreator.newBSPJobID.withId("test_active_sched").
                                      withId(7).build
    new Job.Builder().setId(jobId).
                      setName("test-scheduler").
                      withTarget("groom1").
                      withTaskTable.
                      build
  }

  def createPassiveJob(): Job = {
    val jobId = IDCreator.newBSPJobID.withId("test_passive_sched").
                                      withId(9).build
    new Job.Builder().setId(jobId).
                      setName("test-scheduler").
                      withTaskTable.
                      build
  }

  it("test schedule tasks") {
    LOG.info("Actively schedule tasks")
    sched = system.actorOf(Props(classOf[MockSched], conf, "groom1", prob.ref))
    sched ! AddProxy
    prob.expectMsg(ProxyAdded)
    sched ! Dispense(createActiveJob)
    sched ! GetTarget
    prob.expectMsg("groom1")
    sched ! GetJobData
    prob.expectMsg(JobData(0, 1, createActiveJob.getId))
  }

  it("test tasks assign") {
    LOG.info("Passively schedule tasks")
    sched = system.actorOf(Props(classOf[MockSched], conf, 
                                 "groom_127.0.0.1_50000", prob.ref))
    LOG.info("MockSched and TestProb are created! sched: "+sched+
             ", ref: "+prob.ref)
    sched ! AddProxy
    prob.expectMsg(ProxyAdded)
    sched ! Dispense(createPassiveJob)
    Thread.sleep(5*1000)
    prob.expectMsg("groom_127.0.0.1_50000")
  }
}
