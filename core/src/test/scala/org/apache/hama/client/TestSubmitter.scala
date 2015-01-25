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
package org.apache.hama.client

import akka.actor.ActorRef
import akka.actor.Props
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import java.io.IOException
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.DoubleWritable
import org.apache.hadoop.io.Text
import org.apache.hama.HamaConfiguration
import org.apache.hama.TestEnv
import org.apache.hama.SystemInfo
import org.apache.hama.bsp.BSPJob
import org.apache.hama.bsp.BSPJobID
import org.apache.hama.bsp.FileOutputFormat
import org.apache.hama.bsp.NullInputFormat
import org.apache.hama.bsp.TextOutputFormat
import org.apache.hama.bsp.v2.BSP
import org.apache.hama.bsp.v2.BSPPeer
import org.apache.hama.conf.ClientSetting
import org.apache.hama.conf.Setting
import org.apache.hama.logging.CommonLog
import org.apache.hama.master.BSPMaster
import org.apache.hama.message.compress.SnappyCompressor
import org.apache.hama.sync.SyncException
import org.apache.hama.util.JobUtil
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.collection.immutable.IndexedSeq

object PiEstimator {

  val TMP_OUTPUT = new Path("/tmp/pi-" + System.currentTimeMillis) 

  val iterations = 10000

  class MyEstimator extends BSP with CommonLog {
    var masterTask: Option[String] = None

    @throws(classOf[IOException])
    @throws(classOf[SyncException])
    override def setup(peer: BSPPeer) = 
      masterTask = Option(peer.getPeerName(peer.getNumPeers / 2))

    @throws(classOf[IOException])
    @throws(classOf[SyncException])
    override def bsp(peer: BSPPeer) {
      var in: Int = 0
      for(iteration <- 0 until iterations) {
        val x = 2.0 * Math.random - 1.0
        val y = 2.0 * Math.random - 1.0
        if (Math.sqrt(x * x + y * y) < 1.0) in += 1
      }
      val data = (4.0 * in/ iterations).toDouble
      masterTask.map { m => peer.send(m, new DoubleWritable(data)) }
    }

    @throws(classOf[IOException])
    override def cleanup(peer: BSPPeer) = masterTask.map { m => 
      if(peer.getPeerName.equals(m)) {
        var pi = 0.0d
        val numPeers = peer.getNumCurrentMessages
        var received = new DoubleWritable 
        while ({ received = peer.getCurrentMessage.asInstanceOf[DoubleWritable] 
                 null != received }) {
          pi += received.get
        }
        pi = pi / numPeers
        //peer.write(new Text("Estimated value of PI is"),  TODO: io
                            //new DoubleWritable(pi))
      }
    } 
  }
}

class MockM(setting: Setting, tester: ActorRef) 
      extends BSPMaster(setting, "testMaster") with JobUtil {

  val conf = new HamaConfiguration

  override def join(nodes: IndexedSeq[SystemInfo]) { } 

  override def subscribe(stakeholder: ActorRef) { }

  override def processClientRequest(from: ActorRef) = 
    tester ! Response(jobId, sysDir, 1024)

  def sysDir: Path = new Path(conf.get("bsp.system.dir", 
    "/tmp/hadoop/bsp/system"))

  def jobId: BSPJobID = createJobId("test", 1)

  override def receive = opened
} 

final case class Assign(m: ActorRef)
final case object Unassign

class MockSubmitter(setting: Setting) extends Submitter(setting) {

  override def initializeServices { /* disable retry */ }

  def testMsg: Receive = {
    case Assign(m) => {
      LOG.info("Call afterLinked for simulating master reply.")
      afterLinked("discover", m)
    }
    case Unassign => masterProxy = None
  }
 
  override def receive = testMsg orElse super.receive
   
}

object MockSetting {

  def apply(): Setting = new MockSetting(Setting.client).asInstanceOf[Setting]
}

class MockSetting(setting: Setting) extends ClientSetting(setting.hama) {

  override def config(): Config = ConfigFactory.load

}

@RunWith(classOf[JUnitRunner])
class TestSubmitter extends TestEnv("TestSubmitter") with JobUtil {

  import PiEstimator._

  def bspJob(): BSPJob = {
    val conf = new HamaConfiguration
    val bsp = new BSPJob(conf, classOf[PiEstimator.MyEstimator])
    bsp.setCompressor(classOf[SnappyCompressor]) 
    bsp.setCompressionThreshold(40)
    bsp.setJobName("Pi Estimation Example")
    bsp.setBSPClass(classOf[MyEstimator])
    bsp.setInputFormat(classOf[NullInputFormat])
    bsp.setOutputKeyClass(classOf[Text])
    bsp.setOutputValueClass(classOf[DoubleWritable])
    //bsp.setOutputFormat(classOf[TextOutputFormat])
    FileOutputFormat.setOutputPath(bsp, TMP_OUTPUT)
    bsp.setNumBspTask(5)
    bsp
  }

  def expectedSysDir: Path = new Path("/tmp/hadoop/bsp/system")

  def expectedMaxTasks = 1024

  def configMaster(setting: Setting): Setting = {
    setting.hama.setBoolean("master.need.register", false)
    setting.hama.setBoolean("master.fs.cleaner.start", false)
    setting
  }

  def configClient(setting: Setting): Setting = {
    setting.hama.set("client.main", classOf[MockSubmitter].getName)
    setting
  }

  it("test submitter functions.") {
    val master = configMaster(Setting.master)
    Submitter.start(configClient(MockSetting()))
    Submitter.submit(bspJob)
    val submitter = Submitter.submitter 
    Submitter.system.map { sys => 
      val m = sys.actorOf(Props(classOf[MockM], master, tester), "mockMaster")
      submitter.map { client => client ! Assign(m) }
    }
    expect(Response(createJobId("test", 1), expectedSysDir, expectedMaxTasks))
    LOG.info("Done testing Submitter functions!")    
  }
}
