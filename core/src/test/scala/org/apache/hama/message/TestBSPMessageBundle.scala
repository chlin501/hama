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
package org.apache.hama.message

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.util.concurrent.ConcurrentLinkedQueue
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.Writable
import org.apache.hama.HamaConfiguration
import org.apache.hama.message.compress.BSPMessageCompressor
import org.apache.hama.message.queue.MemoryQueue
import org.apache.hama.TestEnv
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.collection.JavaConversions._


@RunWith(classOf[JUnitRunner])
class TestBSPMessageBundle extends TestEnv("TestBSPMessageBundle") {

  def createBundle[M <: Writable](): BSPMessageBundle[M] = {
    val bundle = new BSPMessageBundle[M]() 
    bundle.setCompressor(BSPMessageCompressor.get(testConfiguration),
                         BSPMessageCompressor.threshold(Option(testConfiguration)))
    bundle
  }
  
  def serialize[M <: Writable](bundle: BSPMessageBundle[M]): Array[Byte] = {
    val bout = new ByteArrayOutputStream()
    val dout  = new DataOutputStream(bout)
    bundle.write(dout)
    val bytes = bout.toByteArray
    dout.close
    bytes
  }  

  def deserialize[M <: Writable](bytes: Array[Byte]): BSPMessageBundle[M] = {
    val bin = new ByteArrayInputStream(bytes) 
    val din = new DataInputStream(bin)
    val bundle = new BSPMessageBundle[M]()
    bundle.readFields(din)
    din.close
    bundle
  }

  it("test bsp message bundle equality.") {
    val msg1 = new Text("Apache")
    val msg2 = new Text("Hama")
    val msg3 = new Text("BSP")
    val msg3x = new Text("BSPx")

    val bundle1 = createBundle[Text]()
    bundle1.addMessage(msg1, msg2, msg3)

    val bundle2 = createBundle[Text]()
    bundle2.addMessage(msg1, msg2, msg3)
    LOG.info("Verify equality between bundles ...")
    assert(bundle1.equals(bundle2)) 

    val bundle3 = createBundle[Text]()
    bundle3.addMessage(msg1, msg2, msg3x)
    assert(!bundle1.equals(bundle3)) 

    LOG.info("Serilaize, deserialize bsp message bundle. Then test equality ..")
    val bytes1 = serialize(bundle1)
    val bundlea = deserialize(bytes1) 
    assert(bundle1.equals(bundlea))

    val bytes2 = serialize(bundle2)
    val bundleb = deserialize(bytes2) 
    assert(bundle2.equals(bundleb))

    val bytes3 = serialize(bundle3)
    val bundlec = deserialize(bytes3) 
    assert(bundle3.equals(bundlec))

    // if bundles are equals, e.g., bundle1 and bundle2, queue should have only
    // one item retained.
    val queue = new MemoryQueue[BSPMessageBundle[Text]]()
    queue.add(bundle1)
    queue.add(bundle2)
    LOG.info("Memory queue's size, expected 1, is {}", queue.size)
    assert(1 == queue.size)
           
    LOG.info("Done testing bsp message bundle equality!")  
  }
}
