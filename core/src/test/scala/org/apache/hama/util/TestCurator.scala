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
package org.apache.hama.util

import akka.actor.ActorSystem
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import org.apache.hama.Tester
import org.apache.hama.TestEnv
import org.apache.hama.zk.LocalZooKeeper
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

class MockCurator extends Curator {

  val LOG = LogFactory.getLog(classOf[MockCurator])

  override def log(message: String) = LOG.info(message)

}

@RunWith(classOf[JUnitRunner])
class TestCurator extends TestEnv(ActorSystem("TestCurator")) 
                  with LocalZooKeeper {

  override protected def beforeAll = launchZk

  override protected def afterAll = {
    closeZk
    super.afterAll
  }


  it("test curator methods") {
    LOG.info("Test curator methods ...")
    val curator = new MockCurator()
    curator.initializeCurator(testConfiguration)
    val materPath = "/bsp/masters/bspmaster/id"
    val masterId = curator.getOrElse(materPath, "master")
    LOG.info("MasterId is "+masterId)
    assert("master".equals(masterId))
    val jobSeqPath = "/bsp/masters/bspmaster/seq"
    val seq = curator.getOrElse(jobSeqPath, 1)
    LOG.info("The job seq value is "+seq)
    assert(1 == seq)
    //expect(Result(true, true, "barrier1"))
  }
}
