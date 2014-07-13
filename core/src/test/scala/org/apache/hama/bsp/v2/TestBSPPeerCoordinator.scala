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
package org.apache.hama.bsp.v2

import akka.actor.ActorSystem
import akka.util.ByteString
import akka.util.ByteStringBuilder
import akka.util.ByteIterator
import org.apache.hadoop.fs.Path
import org.apache.hama.HamaConfiguration
import org.apache.hama.TestEnv
import org.apache.hama.util.JobUtil
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TestBSPPeerCoordinator 
      extends TestEnv(ActorSystem("TestBSPPeerCoordinator")) 
      with JobUtil {

  it("test bsp peer coordinator function.") {
    val task =createTask("test", 2, 23, 3)
    assert(null != task)
    val coordinator = new BSPPeerCoordinator(system)
    coordinator.initialize(testConfiguration, task)
  }
}
