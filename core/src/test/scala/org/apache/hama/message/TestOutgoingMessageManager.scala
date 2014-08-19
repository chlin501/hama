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

import akka.actor.ActorRef
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Writable
import org.apache.hama.HamaConfiguration
import org.apache.hama.TestEnv
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.collection.JavaConversions._

@RunWith(classOf[JUnitRunner])
class TestOutgoingMessageManager extends TestEnv("TestOutgoingMessageManager") {

  it("test outgoing pojo message bundle.") {
    val outgoing = OutgoingMessageManager.get[IntWritable](testConfiguration)
    outgoing.addMessage(Peer.at("BSPPeerSystem1@host123:2139"), 
                        new IntWritable(23))
    outgoing.addMessage(Peer.at("BSPPeerSystem2@my-local-laptop:1235"), 
                        new IntWritable(21))
    outgoing.addMessage(Peer.at("BSPPeerSystem9@host1:29184"), 
                        new IntWritable(11))
    var msgCnt = 0
    asScalaIterator(outgoing.getBundleIterator).foreach( entry => {
      val key = entry.getKey
      val value = entry.getValue
      LOG.info("Message key: "+key+" value: "+value)
      assert(key.getSystemPath.equals("BSPPeerSystem1@host123:2139") ||
             key.getSystemPath.equals("BSPPeerSystem2@my-local-laptop:1235") ||
             key.getSystemPath.equals("BSPPeerSystem9@host1:29184"))
      asScalaIterator(value.iterator).foreach( msg => {
        assert(null != msg)
        LOG.info("Msg value found in BSPMessageBundle: "+msg.get)
        assert(msg.get == 23 || msg.get == 21 || msg.get == 11)
      })
      msgCnt += 1
    })
    LOG.info("Message size should be 3. Actual size is "+msgCnt)
    assert(3 == msgCnt)
  }
}
