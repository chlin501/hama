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
import scala.util.Try
import scala.util.Success
import scala.util.Failure

final case class Checkpoint(taskConf: HamaConfiguration, 
                            data: Writable, 
                            dest: Path)

/**
 * Checkpoint data to HDFS.
 */
class Checkpointer extends Checkpointable with Agent {

  override def checkpoint(data: Writable, dest: Path) {

  }

  protected def internalCheckpoint(taskConf: HamaConfiguration, 
                                   data: Writable, 
                                   dest: Path) {
    val operation = Operation.get(taskConf)
    val out = operation.create(dest)  
    out.isInstanceOf[DataOutputStream] match {
      case false => LOG.warning("Can't write data to {}", dest)
      case true => data.write(out.asInstanceOf[DataOutputStream])
    }
    Try(operation.close(out)) match{
      case Success(result) => LOG.debug("Successfully close stream at {}", dest)
      case Failure(cause) => LOG.error("Fail closing output stream at {}", dest)
    }
  }

  def doCheckpoint: Receive = {
    case Checkpoint(taskConf, data, dest) => {
      internalCheckpoint(taskConf, data, dest)
    }
  }

  override def receive = doCheckpoint orElse unknown
  
}
