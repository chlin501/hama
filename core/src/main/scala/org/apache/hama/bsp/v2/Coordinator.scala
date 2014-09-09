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
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.Writable
import org.apache.hama.bsp.BSPJobID
import org.apache.hama.bsp.Counters
import org.apache.hama.bsp.TaskAttemptID
import org.apache.hama.fs.CacheService
import org.apache.hama.fs.Operation
import org.apache.hama.HamaConfiguration
//import org.apache.hama.io.IO
import org.apache.hama.ProxyInfo
import org.apache.hama.logging.CommonLog
import org.apache.hama.message.BSPMessageBundle
import org.apache.hama.message.MessageManager
import org.apache.hama.message.Messenger
import org.apache.hama.message.PeerCommunicator
import org.apache.hama.monitor.CheckpointerReceiver
import org.apache.hama.monitor.Checkpointable
import org.apache.hama.sync.BarrierClient
import org.apache.hama.sync.PeerSyncClient
import org.apache.hama.sync.SyncException
import org.apache.hama.util.Utils._
import scala.collection.JavaConversions._
import scala.util.Try
import scala.util.Success
import scala.util.Failure

/**
 * This class purely implements BSPPeer interface, and is intended to be used  
 * by @{link Worker} for executing superstep logic.
 *
 * {@link Coordinator} is responsible for providing related services, 
 * including:
 * - messenging
 * - io 
 * - sync
 */
// TODO: use task operator to collect metrics: 
//  - status
//  - start time
//  - finish time
//  - progress, etc.
class Coordinator extends BSPPeer with CheckpointerReceiver 
                                  with Checkpointable 
                                  with Messenger 
                                  with BarrierClient {

  /* This store common configuration */
  protected var conf: HamaConfiguration = new HamaConfiguration

  protected var taskOperator: Option[TaskOperator] = None

  override def configuration(): HamaConfiguration = conf

  /**
   * Configure necessary services for a specific task, including
   * - io
   * - sync
   * - messaging
   * Note: <pre>configuration != task.getConfiguration(). </pre>
   *       The formor comes from child process, the later from the task.
   * @param task contains setting for a specific job; its configuration differs
   *             from conf provided by {@link Container}.
   */
  protected[v2] def configureFor(commonConf: HamaConfiguration, 
                                 operator: Option[TaskOperator],
                                 peerMessenger: ActorRef) {
    taskOperator = operator
    this.conf = commonConf;
    TaskOperator.execute(taskOperator, { (task) =>
      configureForMessenger(configuration, task, peerMessenger) 
    })
    //this.io = ioService[_, _](io, configuration, taskWithStats) 
    TaskOperator.execute(taskOperator, { (task) => 
      localize(configuration, task) 
    })
    TaskOperator.execute(taskOperator, { (task) => 
      settingForTask(configuration, task) 
    })
    TaskOperator.execute(taskOperator, { (task) => 
      configureForBarrier(configuration, task, host, port) 
    })
    TaskOperator.execute(taskOperator, { (task) => 
      firstSync(task.getCurrentSuperstep) 
    })
  }

  /**
   * Internal sync to ensure all peers is registered/ ready.
   * @param superstep indicate the curent superstep value.
   */
  //TODO: should the task's superstep be confiured to 0 instead?
  protected def firstSync(superstep: Long) = 
    TaskOperator.execute(taskOperator, { (task) => {
      syncClient.enterBarrier(task.getId.getJobID, task.getId, superstep)
      syncClient.leaveBarrier(task.getId.getJobID, task.getId, superstep)
      task.increatmentSuperstep
    }})

  /** 
   * - Configure FileSystem's working directory with corresponded 
   * <b>task.getConfiguration()</b>.
   * - And add additional classpath to task's configuration.
   * @param conf is the common setting from bsp peer container.
   * @param task contains setting for particular job computation.
   */
  protected def settingForTask(conf: HamaConfiguration, task: Task) = {
    val taskConf = task.getConfiguration
    Operation.get(taskConf).setWorkingDirectory(
      new Path(Operation.defaultWorkingDirectory(taskConf))
    )
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

  /**
   * Setup io service for a specific task.
  protected def ioService[I, O](io: IO[I, O],
                                conf: HamaConfiguration, 
                                taskWithStats: TaskWithStats): IO[I, O] = {
    val newIO = io match {
      case null => IO.get[I,O](conf)
      case _ => io
    }
    newIO.initialize(getTask.getConfiguration, // tight to a task 
                     getTask.getSplit, 
                     getCounters)
    newIO
  }
   */

  /**
   * Copy necessary files to local (file) system so to speed up computation.
   * @param conf should contain related cache files if any.
   */
  protected def localize(conf: HamaConfiguration, task: Task) = 
    CacheService.moveCacheToLocal(conf)

  /**
   * The host this actor runs on. It may be different from the host that remote 
   * module listens to.
   * @return String name of the host.
   */
  protected def host(): String =  // TODO: move to net package?
    configuration.get("bsp.peer.hostname", 
                      InetAddress.getLocalHost.getHostName) 

  protected def port(): Int = configuration.getInt("bsp.peer.port", 61000) // TODO: move to net pkg?

  //override def getIO[I, O](): IO[I, O] = this.io.asInstanceOf[IO[I, O]]

  @throws(classOf[IOException])
  override def send(peerName: String, msg: Writable) = 
    messenger.send(peerName, msg.asInstanceOf[Writable])

  override def getCurrentMessage(): Writable = 
    messenger.getCurrentMessage.asInstanceOf[Writable]

  override def getNumCurrentMessages(): Int = messenger.getNumCurrentMessages

  @throws(classOf[SyncException])
  protected def enterBarrier() = TaskOperator.execute(taskOperator, { (task) => 
    syncClient.enterBarrier(task.getId.getJobID, task.getId, 
                            task.getCurrentSuperstep)
  }) 

  @throws(classOf[SyncException])
  protected def leaveBarrier() = TaskOperator.execute(taskOperator, { (task) =>
    syncClient.leaveBarrier(task.getId.getJobID, task.getId,
                            task.getCurrentSuperstep)
  })

  protected def doTransfer(peer: ProxyInfo, 
                           bundle: BSPMessageBundle[Writable]): Try[Boolean] = 
    Try(checkThenTransfer(peer, bundle))

  protected def checkThenTransfer(peer: ProxyInfo, 
                                  bundle: BSPMessageBundle[Writable]): 
    Boolean = { messenger.transfer(peer, bundle); true }

  @throws(classOf[IOException])
  override def sync() {
    TaskOperator.execute(taskOperator, { (task) => task.transitToSync })
    //val pack = nextPack(conf)
    val it = messenger.getOutgoingBundles 
    asScalaIterator(it).foreach( entry => {
      val peer = entry.getKey
      val bundle = entry.getValue
      it.remove 
/*
      TaskOperator.execute(taskOperator, { (task) => 
        savePeerBundle(pack, task.getId.toString, getSuperstepCount, peer, 
                       bundle)
      })
*/
      doTransfer(peer, bundle) match {
        case Success(result) => LOG.debug("Successfully transfer messages!")
        case Failure(cause) => LOG.error("Fail transferring messages due "+
                                         "to {}", cause)
      }
    })
    //noMoreBundle(pack, getTask.getId.toString, getSuperstepCount)
     
    enterBarrier()
    clear()
    // TODO: instead of checkpointing outgoing messages with func saveXXXXXX
    //       We should checkpoint localQueue after clear, which in turns calls
    //       messenger.clearOutgoingMessages putting all messages to this 
    //       peer's localQueue.
    //       so in recovery stage, we can simply obtain checkpointed localQueue 
    //       messages back w/ worring about other peer.
    //saveSuperstep(pack)
    leaveBarrier()
    // TODO: record time elapsed between enterBarrier and leaveBarrier, etc.
    TaskOperator.execute(taskOperator, { (task) => task.increatmentSuperstep })
  } 
  
  override def getSuperstepCount(): Long = 
    TaskOperator.execute[Long](taskOperator, { (task) => 
      task.getCurrentSuperstep 
    }, 0L)

  private def initPeers(): Array[String] = 
    TaskOperator.execute[Array[String]](taskOperator, { (task) => 
      syncClient.getAllPeerNames(task.getId) 
    }, Array[String]())

  override def getPeerName(): String = syncClient.getPeerName

  override def getPeerName(index: Int): String = initPeers match {
    case null => null
    case _=> initPeers()(index)
  }

  override def getPeerIndex(): Int = 
    TaskOperator.execute[Int](taskOperator, { (task) => 
      task.getId.getTaskID.getId 
    }, 0)

  override def getAllPeerNames(): Array[String] = initPeers

  override def getNumPeers(): Int = initPeers match {
    case null => 0 
    case _=> initPeers.length
  }

  override def clear() = messenger.clearOutgoingMessages 

  override def getTaskAttemptId(): TaskAttemptID = 
   TaskOperator.execute[TaskAttemptID](taskOperator, { (task) => task.getId }, 
                                       null.asInstanceOf[TaskAttemptID])

  /**
   * This is called after {@link BSP#bsp} finishs its execution in the end.
   * It will close all necessary operations.
   */
   // TODO: close all operations, including io/ message/ sync/ local files in cache, etc.
  protected[v2] def close() = {
    clear 
    syncClient.close 
    messenger.close
  }

}
