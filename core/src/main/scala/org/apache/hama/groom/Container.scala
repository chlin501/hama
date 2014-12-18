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

import java.io.IOException
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
import org.apache.hama.HamaConfiguration
import org.apache.hama.LocalService
import org.apache.hama.RemoteService
import org.apache.hama.bsp.TaskAttemptID
import org.apache.hama.bsp.v2.Coordinator
import org.apache.hama.bsp.v2.Execute
import org.apache.hama.bsp.v2.Task
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
//import org.apache.hama.util.ExecutorLocator
import org.apache.hama.util.TaskCounsellorLocator
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration.DurationInt

protected[groom] final case class Parameters(system: ActorSystem, 
                                             setting: Setting)

object Container extends CommonLog {

  def hamaHome: String = System.getProperty("hama.home.dir")

  def lowercase(): String = classOf[Container].getSimpleName.toLowerCase

  def name(seq: Int): String = "%s%s".format(lowercase, seq)

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

  def initialize(args: Array[String]): Parameters = {
    val setting = customize(Setting.container, args)
    val system = ActorSystem(setting.sys, setting.config)
    Parameters(system, setting)
  }

  def launchFrom(parameters: Parameters) {
    val system = parameters.system
    val setting = parameters.setting
    val containerClass = setting.main 
    val seq = setting.hama.getInt("container.slot.seq", -1)
    LOG.info("Starting BSP slot {} peer system {} at {}:{} with container {} ",
             seq, system.name, setting.host, setting.port, 
             containerClass.getName)
    system.actorOf(Props(containerClass, setting, seq), name(seq))
  }

  @throws(classOf[Throwable])
  def main(args: Array[String]) = try {
    require(null != args && 0 < args.length, "Arguments not supplied!")
    val parameters = initialize(args)
    launchFrom(parameters)
  } catch {
    case e: Exception => {
      LOG.error("Fail launching bsp peer process because {}", e);
      System.exit(-1);
    }
  }
  
  def simpleName(conf: HamaConfiguration, slotSeq: Int): String = 
    conf.get("container.name", classOf[Container].getSimpleName) + slotSeq

}

/**
 * Launched BSP actor in forked process.
 * @param conf contains common setting for the forked process instead of tasks
 *             to be executed later on.
 */
class Container(setting: Setting, slotSeq: Int) extends LocalService 
                                                with RemoteService 
                                                with ActorLocator {

  import Container._
 
  /**
   * Once detecting exceptions thrown by children, report master and stop 
   * actors.
   */
  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 1, withinTimeRange = 1 minute) {
       case _: Exception => {  
         stopAll
         Stop 
       }
    }

  protected var taskCounsellor: Option[ActorRef] = None

  protected var coordinator: Option[ActorRef] = None

  protected val TaskCounsellorName = TaskCounsellor.simpleName(setting.hama)

  /**
   * Stop all realted operations.
   */
  protected def stopAll() {
    coordinator.map { (c) => context.stop(c) }
    coordinator = None
    // TODO: messeenger
    // syncer
  }
 
  override def initializeServices =
    lookup(TaskCounsellorName, locate(TaskCounsellorLocator(setting.hama)))
    //lookup(ExecutorName, locate(ExecutorLocator(setting.hama)))

  override def afterLinked(proxy: ActorRef) = proxy.path.name match {
    case `TaskCounsellorName` => {
      taskCounsellor = Option(proxy)
      proxy ! ContainerReady(slotSeq)
      LOG.info("Container with slot seq {} replies ready to {}!", slotSeq, 
               proxy.path.name)
    } 
    case _ => LOG.warning("Unknown taget {} is linked!", proxy.path.name)
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
      taskCounsellor.map { found => context.unwatch(found) }
      LOG.info("Call context.system.shutdown ...")
      context.system.shutdown
    }
  }

  /**
   * When {@link Executor} is offline, {@link Container} will shutdown
   * itself.
   * @param target actor is {@link Executor}
   */
  override def offline(target: ActorRef) = target.path.name match {
    case `TaskCounsellorName` => self ! ShutdownContainer
    //case coordinator => self ! ShutdownContainer
    //case messenger => self ! ShutdownContainer
    //case syncer => self ! ShutdownContainer
    case _ => LOG.warning("Unexpected actor {} is offline!", target.path.name)
  }

/* TODO: send the latest task to task counsellor and then to scheduler
  def report: Receive = {
    case r: Report => {
      reportToTaskCounsellor(r.getTask)
    }
  }

  def reportToTaskCounsellor(task: Task) = taskCounsellor.map { (e) => 
    e ! new Report(task) 
  }
*/

  override def receive = launchTask orElse resumeTask orElse killTask orElse shutdownContainer orElse actorReply orElse timeout orElse superviseeOffline orElse /*report orElse*/ unknown
}
