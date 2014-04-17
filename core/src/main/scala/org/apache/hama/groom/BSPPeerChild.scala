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

import akka.actor.ActorSystem
import com.typesafe.config.ConfigFactory
import org.apache.hama.LocalService
import org.apache.hama.HamaConfiguration
import org.apache.hama.RemoteService
import org.apache.hama.bsp.TaskAttemptID

object BSPPeerChild {

  @throws(classOf[Throwable])
  def main(args: Array[String]) {
    val defaultConf = new HamaConfiguration()
    if(null == args || 0 == args.length)
      throw new IllegalArgumentException("No arguments supplied when "+
                                         "BSPPeerChild is forked.")
    val host = args(0)
    val port = args(1).toInt
    val taskAttemptId = TaskAttemptID.forName(args(2))
    val superstep = args(3).toInt
    val config = ConfigFactory.parseString("""
    """)
  }
}

class BSPPeerChild(conf: HamaConfiguration) extends LocalService 
                                            with RemoteService {

   override def configuration: HamaConfiguration = conf

   override def name: String = self.path.name

   override def receive = unknown
}
