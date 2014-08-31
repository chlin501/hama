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
// TODO: Task information such as
//  - status
//  - start time
//  - finish time
//  - progress 
// need to find an independent way to update those info. 
class Coordinator extends BSPPeer with CheckpointerReceiver 
                                  with Checkpointable 
                                  with TaskAware 
                                  with Messenger 
                                  with BarrierClient {

  /* services for a particular v2.Task. */
  protected var messenger: Option[MessageManager[Writable]] = None
  protected var syncClient: Option[PeerSyncClient] = None
  protected var conf: HamaConfiguration = new HamaConfiguration

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
                                 bspTask: Task,
                                 peerMessenger: ActorRef) {
    taskWithStats = TaskWithStats(Some(bspTask), Some(new Counters()))
    this.conf = commonConf;
    // TODO: magnet pattern for messenger
    messenger = configureForMessenger(configuration, task, peerMessenger) 
    //this.io = ioService[_, _](io, configuration, taskWithStats) 
    localize(configuration, task)
    settingForTask(configuration, task)
    // TODO: magnet pattern for sync client
    syncClient = configureForBarrier(configuration, task, host, port) 
    firstSync(doIfExists[Task, Int](task, { (found) =>
      found.getCurrentSuperstep
    }, 0))
  }

  /**
   * Internal sync to ensure all peers is registered/ ready.
   * @param superstep indicate the curent superstep value.
   */
  //TODO: should the task's superstep be confiured to 0 instead?
  protected def firstSync(superstep: Long) = task match {
    case Some(found) => {
      doIfExists[PeerSyncClient, Unit](syncClient, { (client) => 
        client.enterBarrier(found.getId.getJobID, found.getId, superstep)
      }, Unit)
      doIfExists[PeerSyncClient, Unit](syncClient, { (client) => 
        client.leaveBarrier(found.getId.getJobID, found.getId, superstep)
      }, Unit)
      found.increatmentSuperstep 
    }
    case None => 
  }

  /** 
   * - Configure FileSystem's working directory with corresponded 
   * <b>task.getConfiguration()</b>.
   * - And add additional classpath to task's configuration.
   * @param conf is the common setting from bsp peer container.
   * @param task is contains setting for particular job computation.
   */
  protected def settingForTask(conf: HamaConfiguration, task: Option[Task]) = 
    doIfExists[Task, Unit](task, { (found) => {
      val taskConf = found.getConfiguration
      Operation.get(taskConf).setWorkingDirectory(
        new Path(Operation.defaultWorkingDirectory(taskConf))
      )
      val libjars = CacheService.moveJarsAndGetClasspath(conf) 
      libjars match {
        case null => LOG.warning("No jars to be included for "+found.getId)
        case _ => {
          LOG.info("Jars to be included in classpath are "+
                   libjars.mkString(", "))
          taskConf.setClassLoader(new URLClassLoader(libjars, 
                                                     taskConf.getClassLoader))
        }
      } 
    }}, Unit)

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
  protected def localize(conf: HamaConfiguration, task: Option[Task]) = 
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
  override def send(peerName: String, msg: Writable) = messenger match {
    case Some(found) => found.send(peerName, msg.asInstanceOf[Writable])
    case None =>
  }

  override def getCurrentMessage(): Writable = 
    doIfExists[MessageManager[Writable], Writable](messenger, { (mgr) =>
      mgr.getCurrentMessage.asInstanceOf[Writable]
    }, null.asInstanceOf[Writable])
    

  override def getNumCurrentMessages(): Int = 
    doIfExists[MessageManager[Writable], Int](messenger, { (mgr) =>
      mgr.getNumCurrentMessages
    }, 0)

  @throws(classOf[SyncException])
  protected def enterBarrier() = doIfExists[Task, Unit](task, { (found) => 
    doIfExists[PeerSyncClient, Unit](syncClient, { (client) => 
      client.enterBarrier(found.getId.getJobID, found.getId, 
                          found.getCurrentSuperstep)
    }, Unit) 
  }, Unit)

  @throws(classOf[SyncException])
  protected def leaveBarrier() = doIfExists[Task, Unit](task, { (found) => 
    doIfExists[PeerSyncClient, Unit](syncClient, { (client) => 
      client.leaveBarrier(found.getId.getJobID, found.getId,
                          found.getCurrentSuperstep)
    }, Unit)
  }, Unit)

  protected def doTransfer(peer: ProxyInfo, 
                           bundle: BSPMessageBundle[Writable]): Try[Boolean] = 
    Try(checkThenTransfer(peer, bundle))

  protected def checkThenTransfer(peer: ProxyInfo, 
                                  bundle: BSPMessageBundle[Writable]): Boolean =
       messenger match { 
    case Some(found) => {
      found.transfer(peer, bundle)
      true
    }
    case None => false
  }

  // TODO: need more concise expression. perhaps refactor interface. return java iterator with entry containing peer and bundle looks stupid.
  protected def getBundles(messenger: Option[MessageManager[Writable]]) = 
    doIfExists[MessageManager[Writable], java.util.Iterator[java.util.Map.Entry[ProxyInfo, BSPMessageBundle[Writable]]]](messenger, { (mgr) => mgr.getOutgoingBundles }, null.asInstanceOf[java.util.Iterator[java.util.Map.Entry[ProxyInfo, BSPMessageBundle[Writable]]]])

  @throws(classOf[IOException])
  override def sync() {
    doIfExists[Task, Unit](task, {(found) => found.transitToSync }, Unit)
    val pack = nextPack(conf)
    val it = getBundles(messenger)

    it match {
      case null =>
        throw new IllegalStateException("MessageManager's outgoing bundles is "+
                                        "null!")
      case _ => {
        asScalaIterator(it).foreach( entry => {
          val peer = entry.getKey
          val bundle = entry.getValue
          it.remove 
          savePeerBundle(pack, doIfExists[Task, String](task, { (found) => 
            found.getId.toString
          }, null.asInstanceOf[String]), getSuperstepCount, peer, bundle)
          doTransfer(peer, bundle) match {
            case Success(result) => LOG.debug("Successfully transfer messages!")
            case Failure(cause) => LOG.error("Fail transferring messages due "+
                                             "to {}", cause)
          }
        })
        //noMoreBundle(pack, getTask.getId.toString, getSuperstepCount)
      }
    }
     
    enterBarrier()
    clear()
    saveSuperstep(pack)
    leaveBarrier()
    // TODO: record time elapsed between enterBarrier and leaveBarrier, etc.
    doIfExists[Task, Unit](task, { (found) => 
      found.increatmentSuperstep 
    }, Unit)
  } 
  
  override def getSuperstepCount(): Long = 
    doIfExists[Task, Long](task, { (found) => found.getCurrentSuperstep }, 0)

  private def initPeers(): Option[Array[String]] = 
    doIfExists[Task, Option[Array[String]]](task, { (found) =>
      doIfExists[PeerSyncClient, Option[Array[String]]](syncClient, { (client)=>
        Some(client.getAllPeerNames(found.getId))
      }, Some(Array[String]()))
    }, Some(Array[String]()))

  override def getPeerName(): String = 
    doIfExists[PeerSyncClient, String](syncClient, { (client) => 
      client.getPeerName
    }, "")

  override def getPeerName(index: Int): String = initPeers match {
    case Some(found) => found(index)
    case None => null
  }

  override def getPeerIndex(): Int = doIfExists[Task, Int](task, { (found) => {
    found.getId.getTaskID.getId 
  }}, -1)

  override def getAllPeerNames(): Array[String] = initPeers.getOrElse(null)

  override def getNumPeers(): Int = initPeers match {
    case Some(found) => found.length
    case None => 0
  }

  override def clear() = doIfExists[MessageManager[Writable], Unit](
    messenger, { (mgr) => mgr.clearOutgoingMessages }, Unit
  )

  override def getTaskAttemptId(): TaskAttemptID = 
    doIfExists[Task, TaskAttemptID](task, { (found) => 
      found.getId 
    }, null.asInstanceOf[TaskAttemptID])

  /**
   * This is called after {@link BSP#bsp} finishs its execution in the end.
   * It will close all necessary operations.
   */
   // TODO: close all operations, including io/ message/ sync/ local files in cache, etc.
  protected[v2] def close() = {
    clear 
    doIfExists[PeerSyncClient, Unit](syncClient, { (client) => 
      client.close 
    }, Unit)
    doIfExists[MessageManager[Writable], Unit](messenger, { (mgr) => 
      mgr.close
    }, Unit)
  }

}
