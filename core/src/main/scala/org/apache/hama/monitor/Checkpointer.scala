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

import java.io.DataOutputStream
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Writable
import org.apache.hama.Agent
import org.apache.hama.fs.Operation
import org.apache.hama.HamaConfiguration
import org.apache.hama.message.BSPMessageBundle
import org.apache.hama.ProxyInfo

sealed trait CkptOp
final case class CommonConfig(conf: HamaConfiguration) extends CkptOp

/**
 * Checkpoint data to HDFS.
 */
class Checkpointer extends Agent {

  protected var conf: Option[HamaConfiguration] = None

  def commonConfig: Receive = {
    case CommonConfig(conf) => this.conf = config(conf)
  }
  
  protected[monitor] def config(conf: HamaConfiguration): 
    Option[HamaConfiguration] = conf match {
      case null => None
      case _ => Some(conf)
  }

  override def receive = commonConfig orElse unknown
  
}
