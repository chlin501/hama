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
package org.apache.hama.master

import org.apache.hama.TestEnv
import org.apache.hama.ProxyInfo
import org.apache.hama.conf.Setting
import org.apache.hama.util.MockCurator
import org.apache.hama.zk.LocalZooKeeper
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TestRegistrator extends TestEnv("TestRegistrator") with LocalZooKeeper {

  val masterName = "bspMaster"
  val masterSys = "BSPMasterSystem"
  val masterHost = "host1034"
  val masterPort: Int = 10428
  override protected def beforeAll = launchZk

  override protected def afterAll = {
    closeZk
    super.afterAll
  }

  it("test master registration methods") {
    val setting = Setting.master
    val conf = setting.hama
    conf.set("master.name", masterName)
    conf.set("master.actor-system.name", masterSys)
    conf.set("master.host", masterHost)
    conf.setInt("master.port", masterPort)
    val reg = Registrator(setting)
    reg.register

    val curator = MockCurator(testConfiguration)
    val ary = curator.list("/masters")
    assert(1 == ary.length)
    val pattern = """(\w+)_(\w+)@(\w+):(\d+)""".r
    ary.map { (child) => {
      LOG.info("Found master znode is {}", child)
      val proxy = pattern.findAllMatchIn(child).map { (matched) =>
        val name = matched.group(1)
        val sys = matched.group(2)
        val host = matched.group(3)
        val port = matched.group(4).toInt
        new ProxyInfo.MasterBuilder(name, setting.hama).build
      }.toArray
      val name = proxy(0).getActorName
      val sys = proxy(0).getActorSystemName
      val host = proxy(0).getHost
      val port = proxy(0).getPort
    
      LOG.info("Master actor name, expected {}, is {}", masterName, name) 
      assert(masterName.equals(name))

      LOG.info("Actor system name, expected {}, is {}", masterSys, sys) 
      assert(masterSys.equals(sys))

      LOG.info("Host value, expected {}, is {}", masterSys, host) 
      assert(masterHost.equals(host))

      LOG.info("Port value, expected {}, is {}", masterPort, port) 
      assert(masterPort == port)
    }}
    curator.closeCurator
  }
}
