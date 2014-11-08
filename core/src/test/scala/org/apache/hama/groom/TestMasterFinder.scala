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
package org.apache.hama.groom

import org.apache.hama.TestEnv
import org.apache.hama.util.MockCurator
import org.apache.hama.zk.LocalZooKeeper
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TestMasterFinder extends TestEnv("TestMasterFinder") with LocalZooKeeper {

  override protected def beforeAll = launchZk

  override protected def afterAll = {
    closeZk
    super.afterAll
  }

  it("test master finder methods") {
    val curator = MockCurator(testConfiguration)
    curator.create("/masters/testBSP_Master1@host887:29417")
    val setting = org.apache.hama.conf.Setting.groom
    val finder = MasterFinder(setting)

    val afterRegistered = finder.masters
    LOG.info("Master contains '{}'", afterRegistered.mkString(","))
    assert(1 == afterRegistered.length)
    
    val master = afterRegistered(0)
    val actorName = master.getActorName
    val sys = master.getActorSystemName
    val host = master.getHost
    val port = master.getPort

    LOG.info("Master's actor name, expected testBSP, is {}", actorName) 
    assert("testBSP".equals(actorName))

    LOG.info("Actor system name, expected Master1, is {}", sys) 
    assert("Master1".equals(sys))

    LOG.info("Host value, expected host887, is {}", host) 
    assert("host887".equals(host))

    LOG.info("Port value, expeected 29417, is {}", port) 
    assert(29417 == port)

    curator.closeCurator
  }
}
