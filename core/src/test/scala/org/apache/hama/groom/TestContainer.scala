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
package org.apache.hama.groom

import akka.actor.ActorRef
import org.apache.hama.bsp.TaskAttemptID
import org.apache.hama.HamaConfiguration
import org.apache.hama.TestEnv
import org.apache.hama.util.JobUtil
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

final case object CheckTaskWorkerVar
sealed trait BooleanVal
final case object True extends BooleanVal
final case object False extends BooleanVal

class MockContainerRestart(conf: HamaConfiguration, 
                           tester: ActorRef) extends Container(conf) {

  override def reply(from: ActorRef, seq: Int, taskAttemptId: TaskAttemptID) = 
    tester ! new Occupied(seq, taskAttemptId)

  override def isOccupied(worker: Option[ActorRef]): Boolean = {
    LOG.info("Is taskWorker created? {} ", isTaskWorkerOccupied)
    isTaskWorkerOccupied match {
      case True => true
      case False => false
    }
  }

  def isTaskWorkerOccupied(): BooleanVal = {
    configuration.getBoolean("test.task.worker.occupied", false) match {
      case true => True
      case false => False
    }
  }

  def checkIfOccupied: Receive = {
    case CheckTaskWorkerVar => {
      LOG.info("Is taskWorker occupied? {} ", isTaskWorkerOccupied)
      tester ! isTaskWorkerOccupied 
    }
  }

  override def receive = checkIfOccupied orElse super.receive  
  
}

@RunWith(classOf[JUnitRunner])
class TestContainer extends TestEnv("TestContainer") with JobUtil {

  override def beforeAll = testConfiguration.setInt("bsp.child.slot.seq", 3)

  it("test container restarting mechanism.") {
    LOG.info("Start testing container restarting mechanim ...")
    val defaultTask = createTask()
    // hama configuration is share. Need pay attention to race condition.
    testConfiguration.setBoolean("test.task.worker.occupied", true)
    val container = createWithArgs("container", classOf[MockContainerRestart], 
                                   testConfiguration, tester) 
    container ! CheckTaskWorkerVar
    expect(True)
    testConfiguration.setBoolean("test.task.worker.occupied", false)
    container ! CheckTaskWorkerVar
    expect(False)
    testConfiguration.setBoolean("test.task.worker.occupied", true)
    container ! new LaunchTask(defaultTask)
    expect(new Occupied(3, defaultTask.getId))

    // TODO: test container ability in restarting peerMessenger and TaskWorker 
    //       when encountering IOException.

    LOG.info("Done testing container.")
  }
}
