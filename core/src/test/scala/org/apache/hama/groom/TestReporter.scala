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

import akka.actor.ActorRef
import org.apache.hama.TestEnv
import org.apache.hama.conf.Setting
import org.apache.hama.monitor.ListService
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

class MockReporter(setting: Setting, groom: ActorRef, tester: ActorRef) 
      extends Reporter(setting, groom) {
  override def currentCollectors(): Array[String] = {
    val collectors = super.currentCollectors
    LOG.info("Collectors available: {}", collectors.mkString(", "))
    tester ! collectors.sorted.toSeq
    collectors
  }
}

class MockGroom(setting: Setting, tester: ActorRef)
      extends GroomServer(setting: Setting, null.asInstanceOf[MasterFinder]) {

  override protected lazy val cluster = null.asInstanceOf[akka.cluster.Cluster]
  override def initializeServices {
    val reporter = getOrCreate(Reporter.simpleName(setting.hama),
                               classOf[MockReporter], setting, self, tester) 
    getOrCreate(TaskCounsellor.simpleName(setting.hama), 
                classOf[TaskCounsellor], setting, self, reporter)
  }

  override def stopServices { }

  override def currentServices(): Array[String] = {
    val available = super.currentServices
    LOG.info("Current services {} available.", available.mkString(", "))
    tester ! available.sorted.toSeq
    available
  }

  def listCollector: Receive = {
    case ListCollector => 
      findServiceBy(Reporter.simpleName(setting.hama)).map { reporter => 
        reporter ! ListCollector
      }
  }

  override def receive = listCollector orElse super.receive
  
}

@RunWith(classOf[JUnitRunner])
class TestReporter extends TestEnv("TestReporter") {


  it("test reporting stats functions.") {
    val groomSetting = Setting.groom
    groomSetting.hama.set("groom.name", classOf[MockGroom].getSimpleName)
    groomSetting.hama.set("groom.main", classOf[MockGroom].getName)
    val expectedServices = Seq(Reporter.simpleName(groomSetting.hama),
                               TaskCounsellor.simpleName(groomSetting.hama)).
                           sorted
    val groom = createWithArgs(groomSetting.name, groomSetting.main, 
                               groomSetting, tester)
     
    groom ! ListService
    expect(expectedServices)
 
    groom ! ListCollector
    expect(Reporter.defaultCollectors.sorted.toSeq) 
  }
}
