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

import akka.actor.ActorRef
import org.apache.hama.TestEnv
import org.apache.hama.conf.Setting
import org.apache.hama.monitor.master.GroomTasksTracker
import org.apache.hama.monitor.master.JobTasksTracker
import org.apache.hama.monitor.master.SysMetricsTracker
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

class MockMonitor(setting: Setting, tester: ActorRef) extends Monitor(setting) {

  override def replyTrackers(from: ActorRef) = {
    val trackers = currentTrackers() 
    LOG.info("Current trackers available include {}", trackers.mkString(","))
    tester ! TrackersAvailable(trackers)
  }

}

@RunWith(classOf[JUnitRunner])
class TestMonitor extends TestEnv("TestMonitor") {

  import Monitor._

  it("test monitor functions.") {
    val master = Setting.master
    val monitor = createWithArgs("monitor", classOf[MockMonitor], master, 
                                 tester)
    monitor ! ListTrackers        
 
    expect(TrackersAvailable(defaultTrackers)) 
  }
}
