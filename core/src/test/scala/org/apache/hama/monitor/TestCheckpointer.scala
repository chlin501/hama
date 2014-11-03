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
package org.apache.hama.monitor

import akka.actor.ActorRef
import java.io.DataInputStream
import java.io.DataOutputStream
import java.io.File
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.BooleanWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.MapWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.Writable
import org.apache.hama.bsp.TaskAttemptID
import org.apache.hama.bsp.v2.Superstep
import org.apache.hama.bsp.v2.BSPPeer
import org.apache.hama.HamaConfiguration
import org.apache.hama.TestEnv
import org.apache.hama.fs.Operation
import org.apache.hama.message.BSPMessageBundle
import org.apache.hama.message.Combiner
import org.apache.hama.message.compress.BSPMessageCompressor
import org.apache.hama.message.Peer
import org.apache.hama.util.JobUtil
import org.apache.hama.zk.LocalZooKeeper
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.collection.JavaConversions._

final case object Done
final case object NotYetFinished

final case class Msgs(arg1: Int, arg2: Int, arg3: Int)

object MockCheckpointer {

  val tmpRootPath = "/tmp/hama/bsp/ckpt"

}

class MockCheckpointer(commConf: HamaConfiguration,
                       taskConf: HamaConfiguration,
                       taskAttemptId: TaskAttemptID,
                       superstep: Long,
                       messenger: ActorRef,
                       superstepWorker: ActorRef,
                       tester: ActorRef) 
      extends Checkpointer(commConf, taskConf, taskAttemptId, superstep, 
                           messenger, superstepWorker) {

  import MockCheckpointer._

  override def getRootPath(taskConf: HamaConfiguration): String = {
    val path = taskConf.get("bsp.checkpoint.root.path", tmpRootPath) 
    LOG.info("Checkpoint file will be writtent to {} directory.", path)
    path 
  }

  override def markIfFinish(): Boolean = {
    val finishOrNot = super.markIfFinish 
    finishOrNot match {
      case true => tester ! Done 
      case false => {
        LOG.info("Not yet finish checkpoint process!")
        tester ! NotYetFinished
      }
    }
    finishOrNot
  }

}

class B extends Superstep {

  def compute(peer: BSPPeer) = println("Test only so compute nothing! ...")

  def next: Class[_ <: Superstep] = null.asInstanceOf[Class[Superstep]]

}

@RunWith(classOf[JUnitRunner])
class TestCheckpointer extends TestEnv("TestCheckpointer") with LocalZooKeeper 
                                                           with JobUtil {
  val superstepCount: Long = 1654
  val threshold = BSPMessageCompressor.threshold(Option(testConfiguration))
  val commConf = testConfiguration
  val taskConf = testConfiguration
  val taskAttemptId = createTaskAttemptId("test", 9, 3, 2)

  val mapVar = Map("superstepCount" -> new LongWritable(superstepCount))
  val next = classOf[B]
  val messages: List[Writable] = List[Writable](new IntWritable(192), 
                                                new IntWritable(112), 
                                                new IntWritable(23))
  val messenger: ActorRef = null 
  val superstepWorker: ActorRef = null


  override protected def beforeAll = launchZk

  override protected def afterAll = {
    closeZk
    super.afterAll
  }

  def bundle(): BSPMessageBundle[Writable] = {
    val bundle = new BSPMessageBundle[Writable]()
    bundle.setCompressor(BSPMessageCompressor.get(commConf), 
                         BSPMessageCompressor.threshold(Option(commConf)))
    bundle
  }

  it("test checkpointer.") {
    val ckpt = createWithArgs("checkpointer", classOf[MockCheckpointer], 
                              commConf, taskConf, taskAttemptId, superstepCount,
                              messenger, superstepWorker, tester)

    ckpt ! LocalQueueMessages(messages)
    expect(NotYetFinished)
    ckpt ! MapVarNextClass(mapVar, next)
    expect(Done)

    val jobDir = new File(MockCheckpointer.tmpRootPath, 
                          taskAttemptId.getJobID.toString)
    val superstepDir = new File(jobDir, ""+superstepCount)
    val msgPath = new File(superstepDir, taskAttemptId.toString+"."+"msg") 
    LOG.info("Does path to {} exist? {}", msgPath, msgPath.exists)
    assert(msgPath.exists) 
    val supPath = new File(superstepDir, taskAttemptId.toString+"."+"sup") 
    LOG.info("Does path to {} exist? {}", supPath, supPath.exists)
    assert(supPath.exists) 

    val operation = Operation.get(commConf)
      
    val msgBundle = bundle
    val msg4Read = operation.open(new Path(msgPath.getPath))
    msgBundle.readFields(new DataInputStream(msg4Read))
    
    asScalaIterator(msgBundle.iterator).foreach( msg => {
      LOG.info("Msg, expected 192, 112, or 23, in bundle is {}", msg)
      val v = msg.asInstanceOf[IntWritable]
      assert(192 == v.get || 112 == v.get || 23 == v.get) 
    })
    msg4Read.close

    val sup4Read = operation.open(new Path(supPath.getPath))
    val input = new DataInputStream(sup4Read)
    val map = new MapWritable 
    map.readFields(input)
    val count1654 = map.get(new Text("superstepCount")).
                        asInstanceOf[LongWritable]
    LOG.info("Checkpointed superstep count, expected {}, is {}", 
             superstepCount, count1654)
    assert(superstepCount == count1654.get)

    // TODO: reaplce by Supersstep.readFields instead
    val hasNext = input.readBoolean
    LOG.info("Has next superstep (expect true)? {}", hasNext)
    assert(true == hasNext)
    val clazzName = Text.readString(input) 
    LOG.info("Next superstep, expect {}, is {}", next.getName, clazzName)
    assert(clazzName.toString.equals(next.getName))
    sup4Read.close

    LOG.info("Done TestCheckpointer test case!")
  }
}

