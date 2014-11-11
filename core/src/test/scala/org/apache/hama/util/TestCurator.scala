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

import org.apache.hama.HamaConfiguration
import org.apache.hama.TestEnv
import org.apache.hama.zk.LocalZooKeeper
import org.junit.runner.RunWith
import org.scalatest.Assertions._
import org.scalatest.junit.JUnitRunner

object MockCurator {

  def apply(conf: HamaConfiguration): Curator = {
    val curator = new MockCurator()
    curator.initializeCurator(conf)
    curator
  }

}

class MockCurator extends Curator 

@RunWith(classOf[JUnitRunner])
class TestCurator extends TestEnv("TestCurator") with LocalZooKeeper {

  override protected def beforeAll = {
    super.beforeAll
    launchZk
  }

  override protected def afterAll = {
    closeZk
    super.afterAll
  }


  it("test curator methods") {
    LOG.info("Test curator methods ...")
    val curator = MockCurator(testConfiguration)
    val masterPath = "/bsp/masters/bspmaster/id"
    val masterId = curator.getOrElse(masterPath, "master")
    LOG.info("MasterId is {}", masterId)
    assert("master".equals(masterId))

    val jobSeqPath = "/bsp/masters/bspmaster/seq"
    val seq = curator.getOrElse(jobSeqPath, 1)
    LOG.info("The job seq value is {}", seq)
    assert(1 == seq)

    val parent = "/masters/master1"
    val children = Array[String]("actor-system", "host", "port")
    val values = Array("bspmaster", "host123", 1923)
    curator.create(parent, children, values)
    curator.list(parent).foreach( znode => znode match {
      case "actor-system" => {
        val path = parent+"/"+znode
        val sys = curator.get(path, null)
        LOG.info("Value of znode path {} is {}", path, sys)
        assert("bspmaster".equals(sys))
      }
      case "host" => {
        val path = parent+"/"+znode
        val host = curator.get(path, null)
        LOG.info("Value of znode path {} is {}", path, host)
        assert("host123".equals(host))  
      }
      case "port" => {
        val path = parent+"/"+znode
        val port = curator.get(path, -1)
        LOG.info("Value of znode path {} is {}", path, port)
        assert(1923 == port)  
      }
      case rest@_ => throw new RuntimeException("Unexpected value "+rest+" at "+
                                                parent+"/"+znode)
    })
  }
}
