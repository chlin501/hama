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

final case class Created(slotSeq: Int, successful: Boolean)

class MockExecutor(testConf: HamaConfiguration, tester: ActorRef) 
      extends Executor(testConf) {

  override def createProcess(cmd: Seq[String], conf: HamaConfiguration) {
    super.createProcess(cmd, conf)
    LOG.info("Is process created successfully? {}", process)
    if(null == process) 
      tester ! Created(slotSeq, false) 
    else 
      tester ! Created(slotSeq, true)
  }

  override def receive = super.receive
  
}

@RunWith(classOf[JUnitRunner])
class TestExecutor extends TestEnv(ActorSystem("TestExecutor")) {

  override protected def beforeAll = {
    super.beforeAll
    testConfiguration.set("bsp.working.dir", testRoot.getCanonicalPath)
  }

  override protected def afterAll = {
    super.afterAll
  }

  def createProcess(name: String): ActorRef = {
    LOG.info("Create actor: "+name)
    createWithArgs(name, 
                   classOf[MockExecutor], 
                   testConfiguration, 
                   tester)
  }

  it("test forking a process") {
    LOG.info("Test forking a process...")
    val process1 = createProcess("process1")
    val process2 = createProcess("process2")
    val process3 = createProcess("process3")
    process1 ! Fork(1, testConfiguration)
    process2 ! Fork(2, testConfiguration)
    process3 ! Fork(3, testConfiguration)
    expectAnyOf(Created(1, true), Created(2, true), Created(3, true))
  }
}
