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
package org.apache.hama.examples

import java.io.IOException
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.DoubleWritable
import org.apache.hadoop.io.Writable
import org.apache.hama.bsp.v2.BSP
import org.apache.hama.bsp.v2.BSPPeer
import org.apache.hama.sync.SyncException
import org.apache.hama.logging.CommonLog

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
