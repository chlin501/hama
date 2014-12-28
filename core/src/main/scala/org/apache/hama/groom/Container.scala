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
import akka.actor.ActorRef
import akka.actor.Cancellable
import akka.actor.OneForOneStrategy
import akka.actor.SupervisorStrategy._
import akka.actor.Props
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import java.net.InetAddress
import org.apache.hadoop.io.Writable
import org.apache.hama.Agent
import org.apache.hama.HamaConfiguration
import org.apache.hama.LocalService
import org.apache.hama.RemoteService
import org.apache.hama.bsp.TaskAttemptID
import org.apache.hama.bsp.v2.Coordinator
import org.apache.hama.bsp.v2.Execute
import org.apache.hama.bsp.v2.InstantiationFailure 
import org.apache.hama.bsp.v2.Task
import org.apache.hama.bsp.v2.TaskFinished
import org.apache.hama.conf.Setting
import org.apache.hama.logging.CommonLog
import org.apache.hama.logging.TaskLogger
import org.apache.hama.message.BSPMessageBundle
import org.apache.hama.message.MessageExecutive
import org.apache.hama.message.Peer
import org.apache.hama.monitor.Report
import org.apache.hama.sync.CuratorBarrier
import org.apache.hama.sync.CuratorRegistrator
import org.apache.hama.sync.PeerClient
import org.apache.hama.util.ActorLocator
import org.apache.hama.util.TaskCounsellorLocator
import org.apache.zookeeper.CreateMode
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration.DurationInt

object Container extends CommonLog {

  def hamaHome: String = System.getProperty("hama.home.dir")

  def customize(setting: Setting, args: Array[String]): Setting = {
    require(4 == args.length, "Some arguments are missing! Arguments: "+
                              args.mkString(","))
    val sys = args(0)
    val listeningTo = args(1) // Note: it may binds to 0.0.0.0 for all inet.
    val port = args(2).toInt
    val seq = args(3).toInt
    require( seq > 0, "Invalid slot seq "+seq+" when forking a child process!")
    setting.hama.set("container.actor-system.name", sys)
    setting.hama.get("container.host", listeningTo)
    setting.hama.setInt("container.port", port)
    setting.hama.setInt("container.slot.seq", seq)
    setting
  }


  def initialize(args: Array[String]) {
    val setting = customize(Setting.container, args)
    val sys = ActorSystem(setting.sys, setting.config)
    sys.actorOf(Props(classOf[Pinger], setting), "pinger")
  }

  @throws(classOf[Throwable])
  def main(args: Array[String]) = try {
    require(null != args && 0 < args.length, "Arguments not supplied!")
    initialize(args)
    LOG.info("Container process is started!")
  } catch {
    case e: Exception => { 
      LOG.error("Fail launching peer process because {}", e);
      System.exit(-1);
    }
  }
  
  def simpleName(conf: HamaConfiguration): String = 
    conf.get("container.name", classOf[Container].getSimpleName) + 
    conf.getInt("container.slot.seq", -1)

}

protected[groom] class Pinger(setting: Setting) extends RemoteService  
                                                   with ActorLocator {

  protected val TaskCounsellorName = TaskCounsellor.simpleName(setting.hama)

  val seq: Int = setting.hama.getInt("container.slot.seq", -1)

  override def initializeServices =
    lookup(TaskCounsellorName, locate(TaskCounsellorLocator(setting.hama)))

  override def afterLinked(proxy: ActorRef) = proxy.path.name match {
    case `TaskCounsellorName` => {
      proxy ! ProcessReady(seq)
      LOG.info("Process with slot seq {} replies ready to {}!", seq, 
               proxy.path.name)
      context stop self
    } 
    case _ => LOG.warning("Unknown target {} is linked!", proxy.path.name)
  }

  override def receive = unknown

}

/**
 * Launched BSP actor in forked process.
 * @param conf contains common setting for the forked process instead of tasks
 *             to be executed later on.
 */
// TODO: when coordinator finishes its execution. notify task counsellor and
//       update slot's task attempt id to none.
class Container(setting: Setting, slotSeq: Int, taskCounsellor: ActorRef) 
      extends LocalService with RemoteService {

  import Container._

  protected var coordinator: Option[ActorRef] = None

  protected val TaskCounsellorName = TaskCounsellor.simpleName(setting.hama)

  /**
   * Capture exceptions thrown by coordinator, etc.
   * Report to master and stop actors when necessary.
   *
   * InstantiationFailure will be restarted thrice, and then stop coordinator. 
   * Offline function will stop container, by which task counsellor will capture
   * and report to master.
   */
  override val supervisorStrategy = 
    OneForOneStrategy(maxNrOfRetries = 3, withinTimeRange = 3 minutes) {
      case e: InstantiationFailure => Restart
      case _: Exception => {  
        stopAll 
        Stop 
      }
    }

  /**
   * Stop all realted operations.
   */
  protected def stopAll() {
    coordinator.map { (c) => context.stop(c) }
    coordinator = None
    // TODO: messeenger
    // syncer
  }

  /**
   * Check if the task worker is running. true if a worker is running; false 
   * otherwise.
   * @param worker wraps the task worker as option.
   * @return Boolean denotes worker is running if true; otherwise false.
   */
  protected def isOccupied(coordinator: Option[ActorRef]): Boolean = 
    coordinator.map { v => true }.getOrElse(false)

  protected def reply(from: ActorRef, seq: Int, taskAttemptId: TaskAttemptID) = 
    from ! new Occupied(seq, taskAttemptId) 

  /**
   * - Create coordinator 
   * - When that actor finishes setup, sending ack back to task counsellor.
   * @param Receive is partial function.
   */
  def launchTask: Receive = {
    case action: LaunchTask => if(!isOccupied(coordinator)) {
      doLaunch(action.task)
      postLaunch(slotSeq, action.task.getId, sender)
    } else reply(sender, slotSeq, action.task.getId)
  }

  /**
   * Start executing the task in another actor.
   * @param task that is supplied to be executed.
   */
  def doLaunch(task: Task) { 
    val tasklog = spawn("taskLogger%s".format(slotSeq),  
                        classOf[TaskLogger], 
                        hamaHome, 
                        task.getId, 
                        slotSeq)
    context watch tasklog 

    val messenger = spawn("messenger-"+Peer.nameFrom(setting.hama), 
                          classOf[MessageExecutive[BSPMessageBundle[Writable]]],
                          slotSeq, 
                          task.getId,
                          self, 
                          tasklog)
    context watch messenger

    val peer = spawn("syncer", 
                     classOf[PeerClient], 
                     setting.hama, 
                     task.getId,
                     CuratorBarrier(setting.hama, task.getId, task.getTotalBSPTasks),
                     CuratorRegistrator(setting.hama),
                     tasklog)
    context watch peer

/*
    this.coordinator = Option(spawn("coordinator", 
                                    classOf[Coordinator], 
                                    setting.hama, 
                                    task, 
                                    self, 
                                    messenger,
                                    peer,
                                    tasklog))

    this.coordinator.map { c => 
      c ! Execute 
      context watch c
    } // TODO: move Execute to initializeServices
*/
  }

  def postLaunch(slotSeq: Int, taskAttemptId: TaskAttemptID, from: ActorRef) {}

  /**
   * - Asynchronouly create task with an actor.
   * - When that actor finishes setup, sending ack back to task counsellor.
   * @param Receive is partial function.
   */
  def resumeTask: Receive = {
    case action: ResumeTask => if(!isOccupied(coordinator)) {
      doResume(action.task)
      postResume(slotSeq, action.task.getId, sender)
    }
  }

  def doResume(task: Task) {
    LOG.info("function doResume is not yet implemented!") // TODO:
  }

  def postResume(slotSeq: Int, taskAttemptId: TaskAttemptID, from: ActorRef) { }

  /**
   * Kill the task that is running.
   * @param Receive is partial function.
   */
  def killTask: Receive = {
    case action: KillTask => {
      doKill(action.taskAttemptId)
      postKill(slotSeq, action.taskAttemptId, sender)
    }
  }

  def doKill(taskAttemptId: TaskAttemptID) = stopAll // TODO: any other operations?

  def postKill(slotSeq: Int, taskAttemptId: TaskAttemptID, from: ActorRef) = 
    from ! new KillAck(slotSeq, taskAttemptId)

  override def stopServices() {
    stopAll
    super.postStop
  }

  /**
   * Shutdown the system. The spawned process will be stopped as well.
   * @return Receive is partial function.
   */
  def shutdownContainer: Receive = {
    case ShutdownContainer => {
      context.unwatch(taskCounsellor) 
      LOG.info("Call context.system.shutdown ...")
      context.system.shutdown
    }
  }

  /**
   * When {@link TaskCounsellor} is offline, {@link Container} will shutdown
   * itself.
   * @param target actor is {@link Executor}
   */
  override def offline(target: ActorRef) = target.path.name match {
    case `TaskCounsellorName` => self ! ShutdownContainer
    // TODO: check messenger, tasklog, syncer  
    // case `TaskLog` => self ! ShutdownContainer
    // case `Syncer` => self ! ShutdownContainer
    // case `Messenger` => self ! ShutdownContainer
    case _ => LOG.warning("Unexpected actor {} is offline!", target.path.name)
  }

  protected def taskFinsihed: Receive = {
    case finished: TaskFinished => taskCounsellor ! finished 
  }

  protected def report: Receive = {
    case taskReport: Report => taskCounsellor ! taskReport 
  }

  override def receive = launchTask orElse resumeTask orElse killTask orElse shutdownContainer orElse actorReply orElse timeout orElse superviseeOffline orElse report orElse unknown

}
