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
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.Writable
import org.apache.hama.TestEnv
import org.apache.hama.conf.Setting
import org.apache.hama.monitor.ListService
import org.apache.hama.monitor.Stats
import org.apache.hama.monitor.master.GroomsTracker
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

final case class ForwardStats(stats: Stats)

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

  val GTracker = classOf[GroomsTracker].getName

  override protected lazy val cluster = null.asInstanceOf[akka.cluster.Cluster]

  override def initializeServices {
    val reporter = getOrCreate(Reporter.simpleName(setting.hama),
                               classOf[MockReporter], setting, self, tester) 
    getOrCreate(TaskCounsellor.simpleName(setting.hama), 
                classOf[TaskCounsellor], setting, self, reporter)
  }

  override def stopServices { }

  override def listServices(from: ActorRef) = from.path.name match {
    case "deadLetters" => {
      val available = currentServices
      LOG.info("Request from {} asking current services available: {}", 
               from.path.name, available.mkString(", "))
      tester ! available.sorted.toSeq
    }
    case _ => LOG.info("{} requests for listing current services.", 
                       from.path.name)
  }

  def listCollector: Receive = {
    case ListCollector => 
      findServiceBy(Reporter.simpleName(setting.hama)).map { reporter => 
        reporter ! ListCollector
      }
  }

  override def forwardStats(stats: Stats) = stats.dest match {
    case GTracker => stats.data.toString match {
      case "Test sampe!" => {
        LOG.info("Stats delegated to GroomServer has dest {} data {}", 
                 stats.dest, stats.data.toString)
        tester ! stats.dest
        tester ! stats.data.toString
      }
      case _ => LOG.info("Rest stats destined to GroomsTracker is {}", stats)
    }
    case _ => LOG.info("Stats destined to other trackers {}", stats)
  }

  def forwardStatsData: Receive = {
    case ForwardStats(stats) => 
      findServiceBy(Reporter.simpleName(setting.hama)).map { service =>
        service ! stats
      }
  }

  override def receive = forwardStatsData orElse listCollector orElse super.receive
  
}

@RunWith(classOf[JUnitRunner])
class TestReporter extends TestEnv("TestReporter") {

  val groomSetting = {
    val setting = Setting.groom
    setting.hama.set("groom.name", classOf[MockGroom].getSimpleName)
    setting.hama.set("groom.main", classOf[MockGroom].getName)
    setting
  }

  it("test reporting stats functions.") {
    val expectedServices = Seq(Reporter.simpleName(groomSetting.hama),
                               TaskCounsellor.simpleName(groomSetting.hama)).
                           sorted
    val groom = createWithArgs(groomSetting.name, groomSetting.main, 
                               groomSetting, tester)
     
    groom ! ListService
    expect(expectedServices)
 
    groom ! ListCollector
    expect(Reporter.defaultCollectors.sorted.toSeq) 

    val dest = classOf[GroomsTracker].getName
    val data = new Text("Test sampe!").asInstanceOf[Writable]
    val stats = Stats(dest, data)

    groom ! ForwardStats(stats)
    expect(dest)
    expect(data.toString)
  }
}
