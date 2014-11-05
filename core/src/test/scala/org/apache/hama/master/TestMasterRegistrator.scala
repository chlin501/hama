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
import org.apache.hama.conf.Setting
import org.apache.hama.zk.LocalZooKeeper
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class TestMasterRegistrator extends TestEnv("TestMasterRegistrator") 
                            with LocalZooKeeper {

  override protected def beforeAll = launchZk

  override protected def afterAll = {
    closeZk
    super.afterAll
  }

  it("test master registration methods") {
    val reg = new MasterRegistrator(Setting.master)
    assert(reg.masters().isEmpty)
    reg.register

    val afterRegistered = reg.masters
    LOG.info("Master contains '{}'", afterRegistered.mkString(","))
    assert(1 == afterRegistered.length)
    
    val master = afterRegistered(0)
    val sys = master.getActorSystemName
    val host = master.getHost
    val port = master.getPort

    LOG.info("Actor system name, expected MasterSystem, is {}", sys) 
    assert("MasterSystem".equals(sys))

    val defaultHost = java.net.InetAddress.getLocalHost.getHostName
    LOG.info("Host value, expected {}, is {}", defaultHost, host) 
    assert(defaultHost.equals(host))

    LOG.info("Port value, expeected 40000, is {}", port) 
    assert(40000 == port)
  }
}
