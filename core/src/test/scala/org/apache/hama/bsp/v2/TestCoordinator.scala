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
package org.apache.hama.bsp.v2

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.event.Logging
import java.net.InetAddress
import org.apache.hadoop.fs.Path
import org.apache.hama.Agent
import org.apache.hama.groom.Container
import org.apache.hama.HamaConfiguration
import org.apache.hama.logging.TaskLogger
import org.apache.hama.logging.TaskLogging
import org.apache.hama.TestEnv
import org.apache.hama.util.JobUtil
import org.apache.hama.zk.LocalZooKeeper
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration.DurationInt

@RunWith(classOf[JUnitRunner])
class TestCoordinator extends TestEnv("TestCoordinator") with JobUtil 
                                                         with LocalZooKeeper {

  override def beforeAll {
    super.beforeAll
    launchZk
  }

  override protected def afterAll = {
    closeZk
    super.afterAll
  }

  it("test bsp peer coordinator function.") {

    LOG.info("(Not yet implemetned) Done testing Coordinator! ")
  }
}
