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
package org.apache.hama.lang

import akka.actor.ActorRef
import akka.actor.ActorSystem
import org.apache.commons.io.FileUtils
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import org.apache.hama._
import org.apache.hama.groom._
import org.apache.hama.bsp.BSPJobID
import org.apache.hama.bsp.TaskAttemptID
import org.apache.hama.bsp.v2._
import org.apache.hama.bsp.v2.IDCreator._
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

class MockExecutor(conf: HamaConfiguration, ref: ActorRef) 
      extends Executor(conf) {
  
  override def receive = super.receive
  
}

@RunWith(classOf[JUnitRunner])
class TestExecutor extends TestEnv(ActorSystem("TestExecutor")) {

  var process: ActorRef = _

  override protected def afterAll = system.shutdown

  it("test forking a process") {
    LOG.info("Test forking a process...")
    process = createWithArgs("process", 
                              classOf[MockExecutor], 
                              testConfiguration, 
                              tester)
    val slotSeq = 3
    process ! Fork(slotSeq, conf)
  }
}
