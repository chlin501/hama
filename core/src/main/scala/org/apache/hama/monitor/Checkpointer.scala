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
import org.apache.hadoop.io.MapWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.Writable
import org.apache.hama.bsp.TaskAttemptID
import org.apache.hama.Agent
import org.apache.hama.Close
import org.apache.hama.fs.Operation
import org.apache.hama.HamaConfiguration
import org.apache.hama.message.BSPMessageBundle
import org.apache.hama.ProxyInfo
import org.apache.hama.util.Curator
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.collection.JavaConversions._

/**
 * Checkpoint related superstep data to HDFS.
 * This class is created corresponded to a particular task attempt id, so 
 * sending to the same Checkpointer ActorRef indicates checkpointing for the 
 * same task.
 * N.B.: Checkpointer will not be recovered because we assume at least some   
 *       checkpoint will success.
 * @param taskConf is the task configuration.
 * @param taskAttemptId denotes with which task content this checkpointer will
 *                      save.
 * @param superstepCount indicates at which superstep this task right now is.
 */
class Checkpointer(taskConf: HamaConfiguration, 
                   taskAttemptId: String, 
                   superstepCount: Long) extends Agent with Curator {

  // TODO: may need a more netural interface for saving messages, etc. 
  //       to different external storage.
  protected val operation = Operation.get(taskConf) 

  /**
   * Save {@link BSPMessageBundle} to HDFS with path pointed to 
   * <pre>
   * ${bsp.checkpoint.root.path}/<job_id>/<superstep>/<task_attepmt_id>.ckpt
   * </pre>
   */
  def savePeerMessages: Receive = {
    case SavePeerMessages(peer, bundle) => 
      doSavePeerMessages(taskConf, taskAttemptId, superstepCount,
                         peer, bundle)
  }

  protected def getRootPath(): String = taskConf.get("bsp.checkpoint.root.path",
                                                     "/bsp/checkpoint") 

  // TODO: we may need to divide <superstep> into sub category because 
  //       more than 10k znodes may lead to performance slow down for zk 
  //       at the final step marking with ok znode.
  protected def getCkptPath(rootPath: String,
                            jobId: String, 
                            aSuperstepCount: Long,
                            aTaskAttemptId: String, 
                            suffix: String = "ckpt"): String = 
    "%s/%s/%s/%s.%s".format(rootPath, jobId, aSuperstepCount, aTaskAttemptId,
                            suffix)

  protected def formCheckpointPath(aSuperstepCount: Long, 
                                   aTaskAttemptId: String,
                                   suffix: String = "ckpt"): String = {
    val rootPath = getRootPath
    val currentTaskAttemptId = TaskAttemptID.forName(aTaskAttemptId)      
    val jobId = currentTaskAttemptId.getJobID.toString
    getCkptPath(rootPath, jobId, aSuperstepCount, aTaskAttemptId, suffix)
  }

  protected def doSavePeerMessages[M <: Writable](taskConf: HamaConfiguration,
                                                  aTaskAttemptId: String,
                                                  aSuperstepCount: Long,
                                                  peer: ProxyInfo, 
                                                  bundle: BSPMessageBundle[M]) {
    formCheckpointPath(aSuperstepCount, aTaskAttemptId) match {
      case null|"" => 
      case ckptPath@_ => {
        LOG.debug("Save peer and bundle data to {}", ckptPath)
        writePeerAndMessages(new Path(ckptPath), peer, bundle)
      }
    }
  }

  protected def writePeerAndMessages[M <: Writable](
    ckptPath: Path, peer: ProxyInfo, bundle: BSPMessageBundle[M]
  ) = write(ckptPath, (out) => { 
    peer.write(out)
    bundle.write(out)
  })

  // TODO: consider to open ckptPath stream at the beginning, then close stream
  //       when receiving NoMoreBundle message would increase performance.
  protected def write(ckptPath: Path, writeTo: (DataOutputStream) => Unit) = 
    Try(createOrAppend(ckptPath)) match {
      case Success(out) => try { writeTo(out) } finally { out.close }
      case Failure(cause) => 
        LOG.error("Unable to create/ append data at {} for {}", ckptPath, cause)
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

  def saveSuperstep: Receive = {
    case SaveSuperstep(className, variables) => 
      doSaveSuperstep(className, variables, superstepCount, taskAttemptId)
  }

  protected def writeClassNameAndVariables(ckptPath: Path, 
                                           className: Text,
                                           variables: MapWritable) = 
    write(ckptPath, (out) => { 
      className.write(out)
      variables.write(out)
    })

  protected def doSaveSuperstep(className: String, 
                                variables: Map[String, Writable],
                                aSuperstep: Long, 
                                aTaskAttemptId: String) {
    val text = className match {
      case null|"" => new Text()
      case value@_ => new Text(value)
    }

    val mapWritable = variables.isEmpty match {
      case true => new MapWritable
      case false => {
        val writable = new MapWritable
        writable.putAll(mapAsJavaMap(variables.map { e => 
          (new Text(e._1), e._2)
        }))
        writable
      }
    }
 
    formCheckpointPath(aSuperstep, aTaskAttemptId) match {
      case null|"" =>
      case ckptPath@_ => {
        LOG.debug("Save superstep class name and variables data to {}", 
                  ckptPath)
        writeClassNameAndVariables(new Path(ckptPath), text, mapWritable) 
        markFinish(formCheckpointPath(aSuperstep, aTaskAttemptId, "ok"))
      }
    }
    doClose
  }

  /**
   * Write to znode denoting the checkpoint process is completed!
   * @param destPath denotes the path at zk and checkpoint process is completed.
   */
  protected def markFinish(destPath: String) = create(destPath)      

  protected def doClose() = self ! Close

  /**
   * Close this checkpointer.
   * @param Receive is a partial function.
   */
  def close: Receive = {
    case Close => context.stop(self)
  }

  override def receive = savePeerMessages orElse saveSuperstep orElse close orElse unknown
}
