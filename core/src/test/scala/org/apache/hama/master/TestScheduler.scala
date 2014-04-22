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
import akka.actor.ActorSystem
import akka.actor.Props
import akka.event.Logging
import org.apache.hama.TestEnv
import org.apache.hama.HamaConfiguration
import org.apache.hama.groom._
import org.apache.hama.bsp.BSPJobID
import org.apache.hama.bsp.v2._
import org.apache.hama.bsp.v2.IDCreator._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

private final case class JobData(fromQueueSize: Int, 
                                 toQueueSize: Int, 
                                 jobId: BSPJobID)
private final case object GetJobData
private final case object GetTarget
private final case object AddProxy
private final case object ProxyAdded

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
        Set(context.actorOf(Props(classOf[MockTaskManager], 
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
class TestScheduler extends TestEnv(ActorSystem("TestScheduler")) {

  var sched: ActorRef = _

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
    sched = createWithArgs("activeSched", 
                           classOf[MockSched], 
                           conf, 
                           "groom1", 
                           tester)
    sched ! AddProxy
    expect(ProxyAdded)
    sched ! Dispense(createActiveJob)
    sched ! GetTarget
    expect("groom1")
    sched ! GetJobData
    expect(JobData(0, 1, createActiveJob.getId))
  }

  it("test tasks assign") {
    LOG.info("Passively schedule tasks")
    sched = createWithArgs("passiveSched", 
                           classOf[MockSched], 
                           testConfiguration, 
                           "groom_127.0.0.1_50000",
                           tester)
    LOG.info("MockSched and TestProb are created! sched: "+sched+
             ", ref: "+tester)
    sched ! AddProxy
    expect(ProxyAdded)
    sched ! Dispense(createPassiveJob)
    sleep(5.seconds)
    expect("groom_127.0.0.1_50000")
  }
}
