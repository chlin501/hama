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
package org.apache.hama.cluster

import org.apache.hama.TestEnv
import org.apache.hama.master.Master
import org.apache.hama.master.BSPMaster
import org.apache.hama.master.Registrator
import org.apache.hama.groom.Groom
import org.apache.hama.groom.GroomServer
import org.apache.hama.groom.MasterFinder
import org.apache.hama.conf.Setting
import org.apache.hama.zk.LocalZooKeeper
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

class MockBSPMaster(setting: Setting) 
      extends BSPMaster(setting, Registrator(setting)) { 

}

class MockGroomServer(setting: Setting) 
      extends GroomServer(setting, MasterFinder(setting)) {
}

@RunWith(classOf[JUnitRunner])
class TestMasterGroom extends TestEnv("TestMasterGroom") with LocalZooKeeper {

  override def beforeAll {
    super.beforeAll
    launchZk
  }

  override def afterAll {
    closeZk
    super.afterAll
  }
  
  def masterSetting(name: String, main: Class[_], port: Int): Setting = {
    val master = Setting.master
    LOG.info("Configure master with: name {}, main class {}, port {}", 
             name, main, port)
    master.hama.set("master.name", name)
    master.hama.setClass("master.main", classOf[Master], main)
    master.hama.setInt("master.port", port)
    master
  }

  def groomSetting(name: String, main: Class[_], port: Int): Setting = {
    val groom = Setting.groom
    LOG.info("Configure groom with: name {}, main class {}, port {}", 
             name, main, port)
    groom.hama.set("groom.name", name)
    groom.hama.setClass("groom.main", classOf[Groom], main)
    groom.hama.setInt("groom.port", port)
    groom
  }

  it("test master groom communication.") {
    val m = masterSetting("master1", classOf[MockBSPMaster], 40001)
    val g1 = groomSetting("groom1", classOf[MockGroomServer], 50001)
    val g2 = groomSetting("groom2", classOf[MockGroomServer], 50002)

    val master = createWithArgs("master1", classOf[MockBSPMaster], m)
    val groom1 = createWithArgs("groom1", classOf[MockGroomServer], g1)
    val groom2 = createWithArgs("groom2", classOf[MockGroomServer], g2)

  }
}
