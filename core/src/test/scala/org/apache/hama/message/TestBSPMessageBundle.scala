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

import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.Writable
import org.apache.hama.HamaConfiguration
import org.apache.hama.message.compress.BSPMessageCompressor
import org.apache.hama.logging.CommonLog
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

  it("test bsp message bundle.") {
    val msg1 = new Text("Apache")
    val msg2 = new Text("Hama")
    val msg3 = new Text("BSP")

    val msg3a = new Text("BSPx")

    val bundle1 = createBundle[Text]()
    bundle1.addMessage(msg1, msg2, msg3)

    val bundle2 = createBundle[Text]()
    bundle2.addMessage(msg1, msg2, msg3)
    LOG.info("Verify equality between bundles ...")
    assert(bundle1.equals(bundle2)) 

    val bundle3 = createBundle[Text]()
    bundle3.addMessage(msg1, msg2, msg3a)
    assert(!bundle1.equals(bundle3)) 
        
    LOG.info("Done testing bsp message bundle!")  
  }
}
