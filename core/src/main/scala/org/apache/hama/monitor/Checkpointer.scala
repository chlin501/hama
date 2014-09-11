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
import org.apache.hama.bsp.v2.Superstep
import org.apache.hama.Agent
import org.apache.hama.Close
import org.apache.hama.fs.Operation
import org.apache.hama.HamaConfiguration
import org.apache.hama.message.CombinerUtil
import org.apache.hama.message.BSPMessageBundle
import org.apache.hama.message.compress.BSPMessageCompressor
//import org.apache.hama.ProxyInfo
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
 * @param commConf is common configuration.
 * @param taskConf contains configuration specific to a task.
 * @param taskAttemptId denotes with which task content this checkpointer will
 *                      save.
 * @param superstepCount indicates at which superstep this task right now is.
 */
class Checkpointer(commConf: HamaConfiguration, 
                   taskConf: HamaConfiguration, 
                   taskAttemptId: String, 
                   superstepCount: Long) extends Agent with Curator {

  // TODO: may need to use more neutral interface for saving data to different
  //       storage.
  protected val operation = Operation.get(taskConf) 

  protected def getRootPath(taskConf: HamaConfiguration): String = 
    taskConf.get("bsp.checkpoint.root.path", "/bsp/checkpoint") 

  // TODO: we may need to divide <superstep> into sub category because 
  //       more than 10k znodes may lead to performance slow down for zk 
  //       at the final step marking with ok znode.
  protected def mkCheckpointPath(rootPath: String, 
                                 superstepCount: Long, 
                                 taskAttemptId: String,
                                 suffix: String = "ckpt"): String = {
    val currentTaskAttemptId = TaskAttemptID.forName(taskAttemptId)      
    val jobId = currentTaskAttemptId.getJobID.toString
    "%s/%s/%s/%s.%s".format(rootPath, jobId, superstepCount, taskAttemptId,
                            suffix)
  }

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
  protected def close: Receive = {
    case Close => context.stop(self)
  }

  protected def checkpoint: Receive = {
    case Checkpoint(variablesMap, nextSuperstepClass, localMessages) => 
      doCheckpoint(variablesMap, nextSuperstepClass, localMessages)
  }

  /**
   * This function performs following steps for checkpoint.
   * - Create path e.g. hdfs, zk used for checkpoint.
   * - Save variables map, class name, messages to hdfs.
   * - Mark successfully finishing ckpt at zk.
   * - Close checkpoint actor.
   * @param variables are map users exploit to store data during superstep  
   *                  computation.
   * @param next is the class for next superstep computation.
   * @param messages are sent from other peers or by itself for next superstep
   *                 computation.
   */
  protected def doCheckpoint[M <: Writable](variables: Map[String, Writable], 
                                            next: Class[_ <: Superstep], 
                                            messages: List[M]) {
    mkCheckpointPath(getRootPath(taskConf), 
                     superstepCount, 
                     taskAttemptId) match {
      case null|"" => 
      case ckptPath@_ => write(new Path(ckptPath), (out) => { 
        toMapWritable(variables).write(out)
        toText(next).write(out)
        toBundle(messages) match {
          case Some(bundle) => bundle.write(out)
          case None => LOG.error("Can't create BSPMessageBundle for "+
                                 "checkpointer {} at {}", 
                                 taskAttemptId, superstepCount)
        }
      })      
    }
    markFinish(mkCheckpointPath(getRootPath(taskConf), superstepCount, 
                                taskAttemptId, "ok"))
    doClose
  }

  protected def toText(next: Class[_ <: Superstep]): Text = {
    val text = new Text
    text.set(next.getClass.getName)
    text
  } 

  protected def toBundle[M <: Writable](messages: List[M]): 
      Option[BSPMessageBundle[M]] = CombinerUtil.get(Option(commConf)) match {
    case None => None
    case Some(combiner) => BSPMessageCompressor.get(commConf) match {
      case null => None // default compressor is SnappyCompressor
      case compressor@_ => {
        val bundle = getBundle[M](compressor)
        messages.foreach( msg => bundle.addMessage(msg))
        val combined = getBundle[M](compressor)
        combined.setCompressor(compressor, 
                               BSPMessageCompressor.threshold(Option(commConf)))
        val itor = bundle.asInstanceOf[java.lang.Iterable[Nothing]]
        combined.addMessage(combiner.combine(itor))
        Option(combined)
      } 
    }
  }
  
  protected def getBundle[M <: Writable](compressor: BSPMessageCompressor): 
      BSPMessageBundle[M] = {
    val bundle = new BSPMessageBundle[M]()
    bundle.setCompressor(compressor, 
                         BSPMessageCompressor.threshold(Option(commConf)))
    bundle
  }
   
  protected def toMapWritable(variables: Map[String, Writable]): MapWritable = 
    variables.isEmpty match {
      case true => new MapWritable
      case false => {
        val writable = new MapWritable
        writable.putAll(mapAsJavaMap(variables.map { e => 
          (new Text(e._1), e._2)
        }))
        writable
      }
    }

  override def receive = checkpoint orElse close orElse unknown
}
