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
package org.apache.hama.sync

import akka.actor.ActorRef
import org.apache.hama.Agent
import org.apache.hama.HamaConfiguration
import org.apache.hama.TestEnv
import org.apache.hama.bsp.TaskAttemptID
import org.apache.hama.util.JobUtil
import org.apache.hama.util.ZkUtil._
import org.apache.hama.zk.LocalZooKeeper
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.collection.JavaConversions._


class MockBarrier(barrier: CuratorBarrier, tester: ActorRef) extends Agent {

  def e: Receive = {
    case Enter(superstep) => {
      barrier.enter(superstep)
      tester ! WithinBarrier 
    }
  }

  def l: Receive = {
    case Leave(superstep) => {
      barrier.leave(superstep)
      tester ! ExitBarrier 
    }
  }
  
  override def receive = e orElse l orElse unknown
}

@RunWith(classOf[JUnitRunner])
class TestCuratorBarrier extends TestEnv("TestCuratorBarrier") 
                         with LocalZooKeeper 
                         with JobUtil {

  override def beforeAll {
    super.beforeAll
    launchZk
  }
 
  override def afterAll {
    closeZk
    super.afterAll 
  }

  it("test curator distributed double barrier wrapper.") {
    val numBSPTasks = 2
    val taskId1 = createTaskAttemptId("test", 1, 1, 1)
    val taskId2 = createTaskAttemptId("test", 1, 2, 1)
 
    val b1 = CuratorBarrier(testConfiguration, taskId1, numBSPTasks)
    val barrier1 = createWithArgs("barrier1", classOf[MockBarrier], b1, tester)

    val b2 = CuratorBarrier(testConfiguration, taskId2, numBSPTasks)
    val barrier2 = createWithArgs("barrier2", classOf[MockBarrier], b2, tester)

    for (superstep <- 1 to 5) {
      LOG.info("Before Enter superstep {}", superstep)
      barrier1 ! Enter(superstep)
      barrier2 ! Enter(superstep)
      expect(WithinBarrier)
      expect(WithinBarrier)

      LOG.info("WithinBarrier at superstep {}", superstep) 
      barrier1 ! Leave(superstep)
      barrier2 ! Leave(superstep)
      expect(ExitBarrier)
      expect(ExitBarrier)
      LOG.info("ExitBarrier at superstep {}", superstep) 
    }

    LOG.info("Done testing CuratorBarrier ...") 
  }
}

