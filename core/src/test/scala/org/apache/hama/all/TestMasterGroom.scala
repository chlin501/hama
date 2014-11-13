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
package org.apache.hama.all

import akka.actor.ActorRef
import org.apache.hama.MultiNodesEnv
import org.apache.hama.master.BSPMaster
import org.apache.hama.master.Registrator
import org.apache.hama.groom.GroomServer
import org.apache.hama.groom.MasterFinder
import org.apache.hama.conf.Setting
import org.apache.hama.zk.LocalZooKeeper
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

class MockBSPMaster(setting: Setting) 
      extends BSPMaster(setting, Registrator(setting)) { 

  override def enroll(participant: ActorRef) {
    LOG.info("Groom {} joins now ...", participant.path.name)
    super.enroll(participant)
  }

}

class MockGroomServer(setting: Setting) 
      extends GroomServer(setting, MasterFinder(setting)) {
}

@RunWith(classOf[JUnitRunner])
class TestMasterGroom extends MultiNodesEnv("TestMasterGroom") 
                              with LocalZooKeeper {

  override def beforeAll {
    super.beforeAll
    launchZk
  }

  override def afterAll {
    closeZk
    super.afterAll
  }
  
  it("test master groom communication.") {
    val m = masterSetting(name = "master1", 
                          actorSystemName = "BSPCluster", 
                          main = classOf[MockBSPMaster], 
                          port = 40001)
    val g1 = groomSetting(name = "groom1", 
                          actorSystemName = "BSPCluster", 
                          main = classOf[MockGroomServer], 
                          port = 50001)
    val g2 = groomSetting(name = "groom2", 
                          actorSystemName = "BSPCluster", 
                          main = classOf[MockGroomServer], 
                          port = 50002)

    startMaster(m)
    masterActorOf(m, m)

    startGrooms(g1)
    groomActorOf(g1, g1)

    sleep(30.seconds)
  }
}
