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
package org.apache.hama.bsp.v2

import akka.actor.ActorRef
import java.io.IOException
import java.net.InetAddress
import java.net.URLClassLoader
import java.util.Iterator
import java.util.Map.Entry
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Writable
import org.apache.hama.Close
import org.apache.hama.LocalService
import org.apache.hama.HamaConfiguration
import org.apache.hama.ProxyInfo
import org.apache.hama.bsp.BSPJobID
//import org.apache.hama.bsp.Counters 
import org.apache.hama.bsp.TaskAttemptID
import org.apache.hama.fs.CacheService
import org.apache.hama.logging.Logging
import org.apache.hama.logging.LoggingAdapter
import org.apache.hama.logging.TaskLog
import org.apache.hama.logging.TaskLogging
import org.apache.hama.logging.TaskLogger
import org.apache.hama.message.ClearOutgoingMessages
import org.apache.hama.message.GetCurrentMessage
import org.apache.hama.message.GetNumCurrentMessages
import org.apache.hama.message.Send
//import org.apache.hama.message.MessageManager
//import org.apache.hama.message.Messenger
//import org.apache.hama.monitor.CheckpointerReceiver
//import org.apache.hama.monitor.Checkpointable
//import org.apache.hama.sync.BarrierClient
//import org.apache.hama.sync.BarrierMessage
//import org.apache.hama.sync.Enter
import org.apache.hama.sync.GetAllPeerNames
import org.apache.hama.sync.GetNumPeers
import org.apache.hama.sync.GetPeerName
import org.apache.hama.sync.GetPeerNameBy
//import org.apache.hama.sync.PeerSyncClient
//import org.apache.hama.sync.SyncException
import org.apache.hama.sync.WithinBarrier
import org.apache.hama.util.Utils
import scala.collection.JavaConversions._
//import scala.util.Try
//import scala.util.Success
//import scala.util.Failure

//final case class Setup(task: Task)

/**
 * {@link Coordinator} is responsible for providing related services, 
 * including:
 * - messenging
 * - <strik>io</strike>
 * - sync
 */
// TODO: use task operator to collect metrics: 
//  - status
//  - start time
//  - finish time
//  - progress, etc.
// TODO: wrapped by BSPPeerAdapter which calls Coordinator instead!
class Coordinator(conf: HamaConfiguration,  // common conf
                  controller: ActorRef,
                  checkpointer: ActorRef,
                  messenger: ActorRef, 
                  syncClient: ActorRef,
                  tasklog: ActorRef) extends TaskLog {


  /**
   * Create a new task when passing task to the next actor.
   * Note that this task var is not the same instance as that in TaskController,
   * which is the aggregator value for all task value updated.
  var task: Option[Task] = None 
   */

  override def LOG: LoggingAdapter = Logging[TaskLogger](tasklog)

  //def configuration(): HamaConfiguration = conf

  /**
   * Prepare related data for a specific task.
   * Note: <pre>configuration != task.getConfiguration(). </pre>
   *       The formor comes from child process, the later from the task.
   * @param task contains setting for a specific job; task configuration differs
   *             from conf provided by {@link Container}.
  protected def setup(task: Task) {  
    this.task = Option(task)
    localize(conf)
    settingFor(task)  
    firstSync(task)  
  }

   * Copy necessary files to local (file) system so to speed up computation.
   * @param conf should contain related cache files if any.
  protected def localize(conf: HamaConfiguration) = 
    CacheService.moveCacheToLocal(conf)
  
   * - Configure FileSystem's working directory with corresponded 
   * <b>task.getConfiguration()</b>.
   * - And add additional classpath to task's configuration.
   * @param conf is the common setting from bsp peer container.
   * @param task contains setting for particular job computation.
  protected def settingFor(task: Task) = {
    val taskConf = task.getConfiguration
    val libjars = CacheService.moveJarsAndGetClasspath(conf) 
    libjars match {
      case null => LOG.warning("No jars to be included for "+task.getId)
      case _ => {
        LOG.info("Jars to be included in classpath are "+
                 libjars.mkString(", "))
        taskConf.setClassLoader(new URLClassLoader(libjars, 
                                                   taskConf.getClassLoader))
      }
    } 
  }

   * Internal sync to ensure all peers is registered/ ready.
   * @param superstep indicate the curent superstep value.
  //TODO: should the task's superstep be confiured to 0 instead?
  protected def firstSync(task: Task) {
    enterBarrier(task.getId, task.getCurrentSuperstep)
    leaveBarrier(task.getId, task.getCurrentSuperstep)
    // TODO: controller ! IncreamentSuperstep // task.increatmentSuperstep
  }
   */
// bsp peer functions
  @throws(classOf[IOException])
  def send(peerName: String, msg: Writable) = 
    messenger ! Send(peerName, msg.asInstanceOf[Writable])

  def getCurrentMessage(): Writable = 
    Utils.await[Writable](messenger, GetCurrentMessage)

  def getNumCurrentMessages(): Int = 
    Utils.await[Int](messenger, GetNumCurrentMessages)

/*
  override def getSuperstepCount(): Long = task.map { (aTask) =>
    aTask.getCurrentSuperstep 
  }.getOrElse(0).asInstanceOf[Long]
*/

  def getSuperstepCount(): Long = 
    Utils.await[Long](controller, GetSuperstepCount)

  def getPeerName(): String = 
    Utils.await[String](syncClient, GetPeerName)

  def getPeerName(index: Int): String = 
    Utils.await[String](syncClient, GetPeerNameBy(index))

/*
  override def getPeerIndex(): Int = task.map { (task) =>  
    task.getId.getTaskID.getId 
  }.getOrElse(0)
*/
  def getPeerIndex(): Int = Utils.await[Int](controller, GetPeerIndex)

  def getNumPeers(): Int = Utils.await[Int](syncClient, GetNumPeers)

  def getAllPeerNames(): Array[String] = 
    Utils.await[Array[String]](syncClient, GetAllPeerNames)

  def getTaskAttemptId(): TaskAttemptID = 
    Utils.await[TaskAttemptID](controller, GetTaskAttemptId) 

  def clear() = messenger ! ClearOutgoingMessages 

/* TODO: further split into smaller parts. been moved to controller
  @throws(classOf[IOException])
  override def sync() = task.map { (aTask) => {
    aTask.transitToSync 
      enterBarrier(aTask.getId, aTask.getCurrentSuperstep)

    val it = Utils.await[ProxyAndBundleIt](messenger, GetOutgoingBundles)
    transmit(it, 0, aTask.getId.toString) match {
      case true => {
          // TODO: record time elapsed between enterBarrier and leaveBarrier, etc.
          // checkpoint(messenger, nextPack(conf)) TODO: move ckpt to the beg of next superstep
          leaveBarrier(aTask.getId, aTask.getCurrentSuperstep)
          aTask.increatmentSuperstep 
      }
      case false => LOG.error("{} waits for further instruction!", 
                              aTask.getId.toString)
    }
  }}

   * This function transmit messages to other peers up to default retry 3 times.
   * @param it is a {@link java.util.Iterator} object containing 
   *           {@link java.util.Map.Entry} with {@link ProxyInfo} and 
   *           {@link BSPMessageBundle} encapsulated.
   * @param retryCount is a record for retry count.  
   * @param taskAttemptId denotes which task is currently executed.
  protected def transmit(it: ProxyAndBundleIt, retryCount: Int, 
                         taskAttemptId: String): Boolean = {
    asScalaIterator(it).foreach( entry => {
      val peer = entry.getKey
      val bundle = entry.getValue
      it.remove 
      messenger ! Transfer(peer, bundle)
    })

    Utils.await[TransferredState](messenger, "IsTransferredCompleted") match {
      case TransferredCompleted => {
        LOG.debug("Message transfers completely for task {}.", taskAttemptId)
        clear
        true
      }
      case TransferredFailure => {
        LOG.error("Fail transferring messages for task {}!", taskAttemptId)
        if(retry >= retryCount) {
          LOG.info("#{} retransmit messages to remote ", (retryCount+1))
          val it = Utils.await[ProxyAndBundleIt](messenger, GetOutgoingBundles)
          // TODO: delay retry?
          transmit(it, (retryCount+1), taskAttemptId)
        } else {
          //ask controller to recover from the latest superstep at diff node
          controller ! TransferredFailure // attach task?
          LOG.warning("Fail transmitting messages, stop current processing!")
          false
        }
      }   
    }
  }

  protected def retry: Int = conf.getInt("bsp.message.transfer.retry", 3)
   */

  /**
   * Enter barrier synchronization.
   * @param taskAttemptId denotes in which task the current process is. 
   * @param superstep denotes the current superstep.
  @throws(classOf[SyncException])
  protected def enterBarrier(taskAttemptId: TaskAttemptID, superstep: Long) = 
    Utils.await[BarrierMessage](syncClient, Enter(superstep))

   * Leave barrier synchronization.
   * @param taskAttemptId denotes in which task the current process is. 
   * @param superstep denotes the current superstep.
  @throws(classOf[SyncException])
  protected def leaveBarrier(taskAttemptId: TaskAttemptID, superstep: Long) = 
    Utils.await[BarrierMessage](syncClient, Leave(superstep))
   */

  //override def receive = setup orElse unknown
}
