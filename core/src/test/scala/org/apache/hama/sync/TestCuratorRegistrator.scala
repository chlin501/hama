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

import org.apache.hama.TestEnv
import org.apache.hama.bsp.TaskAttemptID
import org.apache.hama.conf.Setting
import org.apache.hama.util.JobUtil
import org.apache.hama.util.Utils
import org.apache.hama.util.ZkUtil._
import org.apache.hama.zk.LocalZooKeeper
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class TestCuratorRegistrator extends TestEnv("TestCuratorRegistrator") 
                        with LocalZooKeeper 
                        with JobUtil {

  val host = Utils.hostname

  override def beforeAll {
    super.beforeAll
    launchZk
  }
 
  override def afterAll {
    closeZk
    super.afterAll 
  }

  it("test peer registration functions.") {
    val taskId1 = createTaskAttemptId("test", 1, 1, 1)
    val taskId2 = createTaskAttemptId("test", 1, 2, 1)
 
    val setting = Setting.container
    val operator1 = CuratorRegistrator(setting)
    operator1.register(taskId1, "BSPPeerSystem1", "host12", 12398)

    val operator2 = CuratorRegistrator(setting)
    operator2.register(taskId2, "BSPPeerSystem3", "dummy2", 3192)

    val peersFoundByOp1 = operator1.getAllPeerNames(taskId1)
    assert(null != peersFoundByOp1)
    LOG.info("Peers found by operator1 => "+peersFoundByOp1.mkString(","))
    assert(2 == peersFoundByOp1.length)

    val peersFoundByOp2 = operator1.getAllPeerNames(taskId2)
    assert(null != peersFoundByOp2)
    LOG.info("Peers found by operator2 => "+peersFoundByOp2.mkString(","))
    assert(2 == peersFoundByOp2.length)

    assert(peersFoundByOp1.sorted.deep == peersFoundByOp2.sorted.deep) 
  }
}

