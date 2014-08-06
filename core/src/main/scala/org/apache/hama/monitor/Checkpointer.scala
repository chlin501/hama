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
import org.apache.hama.bsp.TaskAttemptID
import org.apache.hama.Agent
import org.apache.hama.fs.Operation
import org.apache.hama.HamaConfiguration
import org.apache.hama.message.BSPMessageBundle
import org.apache.hama.ProxyInfo
import scala.util.Failure
import scala.util.Success
import scala.util.Try

/**
 * Checkpoint related superstep data to HDFS.
 * @param taskConf is the task configuration.
 * @param taskAttemptId denotes with which task content this checkpointer will
 *                      save.
 * @param superstepCount indicates at which superstep this task right now is.
 */
class Checkpointer(taskConf: HamaConfiguration, 
                   taskAttemptId: String, 
                   superstepCount: Long) extends Agent {

  protected val operation = Operation.get(taskConf) 

  /**
   * Save {@link BSPMessageBundle} to HDFS with path pointed to 
   * <pre>
   * ${bsp.checkpoint.root.path}/<job_id>/<superstep>/<task_attepmt_id>.ckpt
   * </pre>
   */
  def saveBundle: Receive = {
    case Save(peer, bundle) => doSave(taskConf, taskAttemptId, superstepCount,
                                      peer, bundle)
  }

  protected def doSave[M <: Writable](taskConf: HamaConfiguration,
                                      aTaskAttemptId: String,
                                      aSuperstepCount: Long,
                                      peer: ProxyInfo, 
                                      bundle: BSPMessageBundle[M]) {
    val rootPath = taskConf.get("bsp.checkpoint.root.path", 
                                "/tmp/bsp/checkpoint") 
    val currentTaskAttemptId = TaskAttemptID.forName(aTaskAttemptId)      
    val jobId = currentTaskAttemptId.getJobID.toString
    rootPath match {
      case null|"" => LOG.warning("Root path is missing for {} at {}", 
                                  aTaskAttemptId, aSuperstepCount)
      case path@_ => { 
        val ckptPath = "%s/%s/%s/%s.ckpt".format(path, jobId, aSuperstepCount,
                                                 aTaskAttemptId)
        LOG.debug("Save peer and bundle data to {}", ckptPath)
        Try(createOrAppend(new Path(ckptPath))) match {
          case Success(out) => {
            peer.write(out)
            bundle.write(out)
            out.close 
          }
          case Failure(cause) => {
            LOG.error("Unable to creat/ append data at {} for {}", 
                      ckptPath, cause)
          }
        } 
      }
    } 
  }

  protected def createOrAppend(targetPath: Path): DataOutputStream = {
    operation.exists(targetPath) match {
      case true => operation.append(targetPath).asInstanceOf[DataOutputStream]
      case false => {
        operation.mkdirs(targetPath)
        operation.create(targetPath).asInstanceOf[DataOutputStream]
      }
    }
  }

  //def saveVariables
  //def saveSuperstep

  override def receive = saveBundle orElse unknown
  
}
