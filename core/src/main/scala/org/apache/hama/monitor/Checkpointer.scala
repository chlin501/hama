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

import akka.actor.ActorRef
import java.io.DataOutputStream
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.MapWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.Writable
import org.apache.hama.bsp.TaskAttemptID
import org.apache.hama.bsp.v2.Superstep
import org.apache.hama.LocalService
import org.apache.hama.Close
import org.apache.hama.fs.Operation
import org.apache.hama.HamaConfiguration
import org.apache.hama.message.BSPMessageBundle
import org.apache.hama.message.Combiner
import org.apache.hama.message.compress.BSPMessageCompressor
import org.apache.hama.util.Curator
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.collection.JavaConversions._

/**
 * Checkpoint related superstep data to external storage.
 * This class is created corresponded to a particular task attempt id, so 
 * sending to the same Checkpointer ActorRef indicates checkpointing for the 
 * same task.
 * Note: Checkpointer will not be recovered because we assume at least some   
 *       checkpoints will success.
 * @param commConf is common configuration.
 * @param taskConf contains configuration specific to a task.
 * @param taskAttemptId denotes with which task content this checkpointer will
 *                      save.
 * @param superstepCount indicates at which superstep this task right now is.
 */
class Checkpointer(commConf: HamaConfiguration, 
                   taskConf: HamaConfiguration, 
                   taskAttemptId: TaskAttemptID, 
                   superstepCount: Long, 
                   messenger: ActorRef,
                   superstepWorker: ActorRef) 
      extends LocalService with Curator {

  // TODO: may need to use more neutral interface for saving data to different
  //       storage.
  protected val operation = Operation.get(taskConf) 
  
  /**
   * Record if messages and map variables received or not, regardless of 
   * successfully checkpointing. If anything goes wrong, stop and close it.
   */
  protected var msgsReceived = false

  protected var mapNextReceived = false 

  /**
   * Set to true when messages is successfully written to external storage.
   */
  protected var msgsStatus = false

  /**
   * Set to true when map and next is successfully written to external storage.
   */
  protected var mapNextStatus = false
 
  override def configuration(): HamaConfiguration = commConf

  override def initializeServices = initializeCurator(commConf)

  /**
   * Root path to external storage.
   */
  protected def getRootPath(taskConf: HamaConfiguration): String = 
    taskConf.get("bsp.checkpoint.root.path", "/checkpoint") 

  protected def mkDir(rootPath: String, superstepCount: Long): String = 
    "%s/%s/%s".format(rootPath, taskAttemptId.getJobID.toString, superstepCount)

  // TODO: we may need to divide <superstep> into sub category because 
  //       more than 10k znodes may lead to performance slow down for zk 
  //       at the final step marking with ok znode.
  protected def mkPath(rootPath: String, superstepCount: Long, 
                       suffix: String = "ckpt"): String = 
    "%s/%s/%s/%s.%s".format(rootPath, taskAttemptId.getJobID.toString,
                            superstepCount, taskAttemptId.toString, suffix)

  /**
   * Write data to the output stream, and close the stream when finishing 
   * writing.
   * @param ckptPath points to the dest where data to be written.
   * @param writeTo data output stream.
   * @return Boolean denotes if writing successes or not.
   */
  protected def write(ckptPath: Path, writeTo: (DataOutputStream) => Boolean): 
      Boolean = Try(createOrAppend(ckptPath)) match {
    case Success(out) => { 
      var flag = false 
      try { 
        flag = writeTo(out) 
      } catch { 
        case e: Exception => {
          LOG.error("Exception is thrown when writing to "+ckptPath, e)
          flag = false  
        }
      } finally { 
        out.close // TODO: maybe a better mechansim for closing output stream.
      }
      flag
    }
    case Failure(cause) => {
      LOG.error("Fail to creat or to append data at {} for {}", ckptPath, cause)
      false
    }
  }

  protected def createOrAppend(targetPath: Path): DataOutputStream = {
    operation.exists(targetPath) match {
      case true => {
        LOG.debug("Appending file at {}", targetPath)
        operation.append(targetPath).asInstanceOf[DataOutputStream]
      }
      case false => {
        val parentDir = targetPath.getParent
        LOG.debug("Creating directory at {}", parentDir)
        operation.mkdirs(parentDir)
        LOG.debug("Creating file at {}", targetPath)
        operation.create(targetPath).asInstanceOf[DataOutputStream]
      }
    }
  }

  /**
   * Write to znode denoting the checkpoint process is completed!
   * @param destPath denotes the path at zk and checkpoint process is completed.
   */
  protected def markIfFinish(): Boolean = if(msgsReceived && mapNextReceived) { 
    val dest = ckptDir + "/" + taskAttemptId  
    (msgsStatus && mapNextStatus) match {  // decide fail or success
      case true => create(dest + ".ok") 
      case false => create(dest + ".fail") 
    }
    true
  } else false

  protected def ifClose() = if(msgsReceived && mapNextReceived) {
    self ! Close
  }

  /**
   * Close this checkpointer.
   * @param Receive is a partial function.
   */
  protected def close: Receive = {
    case Close => context.stop(self)
  }
  
  /**
   * MessageExecutive replies with messages in localQueue for checkpoint.
   */
  protected def localQueueMessages: Receive = {
    case LocalQueueMessages(messages) => {
      msgsReceived = true
      msgsStatus = writeMessages(messages) 
      markIfFinish
      ifClose
    }
  }

  protected def ckptDir(): String = mkDir(getRootPath(taskConf), superstepCount)

  /**
   * Checkpoint path with suffix supplied.
   */
  protected def ckptPath(suffix: String): String = ckptDir + "/"+ 
    taskAttemptId + "." + suffix

  protected def writeMessages(messages: List[Writable]): Boolean = 
    write(new Path(ckptPath("msg")), (out) => { 
      toBundle(messages) match {
        case Some(bundle) => { bundle.write(out); true }
        case None => {
          LOG.error("Can't create BSPMessageBundle for checkpointer {} at {}", 
                    taskAttemptId, superstepCount)
          false
        }
      }
    })

  protected def toBundle(messages: List[Writable]): 
    Option[BSPMessageBundle[Writable]] = Combiner.get(Option(commConf)) match {
    case None => {
      val compressor = BSPMessageCompressor.get(commConf)
      val bundle = getBundle(compressor)
      messages.foreach( msg => bundle.addMessage(msg))
      Option(bundle)
    } 
    case Some(combiner) => {
      val compressor = BSPMessageCompressor.get(commConf) 
      val bundle = getBundle(compressor)
      messages.foreach( msg => bundle.addMessage(msg))
      val combined = getBundle(compressor)
      val itor = bundle.asInstanceOf[java.lang.Iterable[Nothing]]
      combined.addMessage(combiner.combine(itor))
      Option(combined)
    }
  }

  protected def getBundle(compressor: BSPMessageCompressor): 
      BSPMessageBundle[Writable] = {
    val bundle = new BSPMessageBundle[Writable]()
    bundle.setCompressor(compressor, 
                         BSPMessageCompressor.threshold(Option(commConf)))
    bundle
  }


  protected def mapVarNextClass: Receive = {
    case MapVarNextClass(map, next) => {
      mapNextReceived = true
      mapNextStatus = writeMapNext(map, next) 
      markIfFinish
      ifClose
    }
  }

  protected def writeMapNext(map: Map[String, Writable], next: Class[_]): 
    Boolean = write(new Path(ckptPath("sup")), (out) => { 
      toMapWritable(map).write(out)
      writeText(next)(out)
      true
    })
  
  // TODO: replace by (immutable) Superstep.write instead 
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

  protected def writeText(next: Class[_])(out: DataOutputStream) = next match {
    case null => out.writeBoolean(false)
    case clazz@_ => { 
      out.writeBoolean(true)
      Text.writeString(out, next.getName)
    }
  }

  /**
   * Ideally this won't happen because viewable will at least return empty 
   * list instead of None.
   */
  protected def notViewable: Receive = {
    case NotViewableQueue => {
      LOG.error("LocalQueue is not an instance of Viewable for task {} "+
                "at superstep {}!", taskAttemptId, superstepCount) 
      close
    }
  }

  override def receive = localQueueMessages orElse notViewable orElse mapVarNextClass orElse close orElse unknown

}

