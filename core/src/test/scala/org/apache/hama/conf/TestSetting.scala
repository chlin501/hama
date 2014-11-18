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
package org.apache.hama.conf

import org.apache.hama.TestEnv
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TestSetting extends TestEnv("TestSetting") {

  it("test setting functions.") {
    val master = Setting.master
    testMaster(master)
    val groom = Setting.groom
    testGroom(groom)

    val m1 = Setting.master
    val g1 = Setting.groom
    val g2 = Setting.groom
    Setting.change("TestBSPSystem", m1, g1, g2)
    assert("TestBSPSystem".equals(m1.sys))
    assert("TestBSPSystem".equals(g1.sys))
    assert("TestBSPSystem".equals(g2.sys))
  }

  def testGroom(setting: Setting) {
    val defaultSys = setting.sys
    val defaultHost = setting.host
    val defaultPort = setting.port
    LOG.info("Default sys {}, host {}, port {}", 
             defaultSys, defaultHost, defaultPort)
    assert("BSPSystem".equals(defaultSys))
    assert(java.net.InetAddress.getLocalHost.getHostName.equals(defaultHost))
    assert(50000 == defaultPort)
    
    setting.hama.set("groom.actor-system.name", "TestGroomActorSystem")
    setting.hama.set("groom.host", "TestGroomHost")
    setting.hama.setInt("groom.port", 9521)
    
    val sys = setting.sys
    val host = setting.host
    val port = setting.port
    LOG.info("After groom is changed: sys {}, host {}, port {}", 
             sys, host, port)

    assert("TestGroomActorSystem".equals(sys))
    assert("TestGroomHost".equals(host))
    assert(9521 == port)
  }

  def testMaster(setting: Setting) {
    val defaultSys = setting.sys
    val defaultHost = setting.host
    val defaultPort = setting.port
    LOG.info("Default sys {}, host {}, port {}", 
             defaultSys, defaultHost, defaultPort)
    assert("BSPSystem".equals(defaultSys))
    assert(java.net.InetAddress.getLocalHost.getHostName.equals(defaultHost))
    assert(40000 == defaultPort)
    
    setting.hama.set("master.actor-system.name", "TestMasterActorSystem")
    setting.hama.set("master.host", "TestMasterHost")
    setting.hama.setInt("master.port", 1234)
    
    val sys = setting.sys
    val host = setting.host
    val port = setting.port
    LOG.info("After master conf is changed: sys {}, host {}, port {}", 
             sys, host, port)

    assert("TestMasterActorSystem".equals(sys))
    assert("TestMasterHost".equals(host))
    assert(1234 == port)
    
  }
}
