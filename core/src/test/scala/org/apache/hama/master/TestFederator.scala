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
import org.apache.hama.monitor.ListService
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

class MockFederator(setting: Setting, master: ActorRef, tester: ActorRef) 
      extends Federator(setting, master) {

  override def currentTrackers(): Array[String] = {
    val trackers = super.currentTrackers
    tester ! trackers.sorted.toSeq
    trackers
  }

}

class MockMaster(setting: Setting, tester: ActorRef)
      extends BSPMaster(setting, null.asInstanceOf[Registrator]) {

  override def initializeServices {
    val conf = setting.hama
    getOrCreate(Federator.simpleName(conf), classOf[MockFederator], setting, 
                self, tester) 
  }

  def listTracker: Receive = {
    case ListTracker => 
      findServiceBy(Federator.simpleName(setting.hama)).map { tracker => 
        tracker ! ListTracker
      }
  }

  override def listServices(from: ActorRef) = from.path.name match {
    case "deadLetters" => {
      val available = currentServices
      LOG.info("{} requests current master services available: {}", 
               from.path.name, available.mkString(","))
      tester ! available.sorted.toSeq
    }
    case _ => LOG.info("Others request for master services available {}", 
                       from.path.name)
  }

  override def receive = listTracker orElse super.receive 

}

@RunWith(classOf[JUnitRunner])
class TestFederator extends TestEnv("TestFederator") {

  val masterSetting = {
    val setting = Setting.master
    setting.hama.set("master.name", classOf[MockMaster].getSimpleName)
    setting.hama.set("master.main", classOf[MockMaster].getName)
    setting
  }

  it("test federator functions.") {
    val expectedServices = Seq(Federator.simpleName(masterSetting.hama))
    val master = createWithArgs(masterSetting.name, masterSetting.main, 
                               masterSetting, tester)
        
    master ! ListService
    expect(expectedServices)
 
    master ! ListTracker 
    expect(Federator.defaultTrackers.sorted.toSeq)
     
  }
}
