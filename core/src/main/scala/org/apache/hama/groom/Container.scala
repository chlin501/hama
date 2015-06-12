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

import akka.actor.Actor
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
import org.apache.hama.bsp.v2.InstantiationFailure 
import org.apache.hama.bsp.v2.Task
import org.apache.hama.bsp.v2.TaskFinished
import org.apache.hama.conf.Setting
import org.apache.hama.logging.CommonLog
import org.apache.hama.logging.TaskLogger
import org.apache.hama.message.BSPMessageBundle
import org.apache.hama.message.MessageExecutive
import org.apache.hama.message.Peer
import org.apache.hama.monitor.TaskReport
import org.apache.hama.sync.CuratorBarrier
import org.apache.hama.sync.CuratorRegistrator
import org.apache.hama.sync.PeerClient
import org.apache.hama.util.ActorLocator
import org.apache.hama.util.TaskCounsellorLocator
import org.apache.zookeeper.CreateMode
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration.DurationInt
import scala.util.Failure
import scala.util.Success
import scala.util.Try

object Container extends CommonLog {

  // TODO: group for global attribute setting (e.g. Setting)
  def hamaHome: String = System.getProperty("hama.home.dir") 

  /**
   * Configure command line arguments to setting.
   * @param setting is the container setting.
   * @param args is the arguments passed in from command line.
   */
  def customize(setting: Setting, args: Array[String]): Setting = {
    require(4 == args.length, "Some arguments are missing! Arguments: "+
                              args.mkString(","))
    val sys = args(0)
    val listeningTo = args(1) // Note: it may binds to 0.0.0.0 for all inet.
    val port = args(2).toInt
    val seq = args(3).toInt
    LOG.info("Process is launched at {}{}@{}:{}", sys, seq, listeningTo, port)
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
    sys.actorOf(Props(classOf[Pinger], setting), Pinger.simpleName(setting))
  }

  @throws(classOf[Throwable])
  def main(args: Array[String]) = try {
    if(null != args) LOG.debug("Process arguments: {}", args.mkString(","))
    require(null != args && 0 < args.length, "Arguments not supplied!")
    initialize(args)
    LOG.info("Container process is started!")
  } catch {
    case e: Exception => { 
      LOG.error("Exception {}", e);
      System.exit(-1);
    }
  }
  
  def simpleName(setting: Setting): String = {
    val seq = setting.getInt("container.slot.seq", -1)
    require(-1 != seq, "Slot seq shouldn't be "+seq+"!")
    setting.get("container.name", classOf[Container].getSimpleName) + seq
  }

}

object Pinger {

  def simpleName(setting: Setting): String = setting.get(
    "container.pinger.name", 
    classOf[Pinger].getSimpleName
  )

}

/**
 * Notify {@link TaskCounsellor} that the child process is up.
 * @param setting is the container setting.
 */
protected[groom] class Pinger(setting: Setting) extends RemoteService  
                                                   with ActorLocator {

  protected val TaskCounsellorName = TaskCounsellor.simpleName(setting)

  override def initializeServices = lookup(TaskCounsellorName, 
    locate(TaskCounsellorLocator(setting)))

  /**
   * Ping with related information once successfully linking to rmeote 
   * {@link TaskCounsellor}.
   */
  override def afterLinked(proxy: ActorRef) = proxy.path.name match {
    case `TaskCounsellorName` => {
      val seq = setting.hama.getInt("container.slot.seq", -1)
      require( -1 != seq, "Container process's slot seq shouldn't be -1!")
      proxy ! ProcessReady(setting.sys, seq, setting.host, setting.port) 
      LOG.info("Container process with slot seq {} replies ready to {}!", seq, 
               proxy.path.name)
    } 
    case _ => LOG.warning("Unknown target {} is linked!", proxy.path.name)
  }

  /**
   * Shutdown the entire system, inclusive of process, when detecting remote
   * {@link TaskCounsellor} offline.
   */
  override def offline(target: ActorRef)  = target.path.name match {
    case `TaskCounsellorName` => { 
      LOG.warning("Shutdown because {} is offline!", target.path.name)
      shutdown
    }
    case _ => LOG.warning("Unknown remote {} offline!", target.path.name)
  }

  override def receive = actorReply orElse timeout orElse superviseeOffline orElse unknown

}

trait Computation extends LocalService { self: Actor => 

  protected var tasklog: Option[ActorRef] = None
  protected var messenger: Option[ActorRef] = None
  protected var peer: Option[ActorRef] = None
  protected var coordinator: Option[ActorRef] = None

  protected def initialize(setting: Setting, task: Task, hamaHome: String,
                           seq: Int, container: ActorRef) {
    // TODO: when testing, hamaHome is null!!!
    val log = spawn(TaskLogger.simpleName(setting), classOf[TaskLogger], 
                    hamaHome, task.getId)
    context watch log 
    tasklog = Option(log)

    val mgr = spawn(MessageExecutive.simpleName(setting), 
                    classOf[MessageExecutive[BSPMessageBundle[Writable]]],
                    setting, seq, task.getId, container, log)
    context watch mgr 
    messenger = Option(mgr)

    val syncer = spawn(PeerClient.simpleName(setting), classOf[PeerClient], 
                 setting, task.getId, 
                 CuratorBarrier(setting, task.getId, task.getTotalBSPTasks),
                 CuratorRegistrator(setting), log)
    context watch syncer
    peer = Option(syncer)

    val c = spawn(Coordinator.simpleName(setting), classOf[Coordinator], 
                  setting, task, container, mgr, syncer, log)

    context watch c 
    coordinator = Option(c)
  }

  /**
   * Force children to stop by calling context.stop(reference).
   */
  protected def destroy() { 
    tasklog.map { log => stop(log) }
    tasklog = None
    messenger.map { mgr => stop(mgr) }
    messenger = None
    peer.map { p => stop(p) }
    peer = None
    coordinator.map { c => stop(c) }
    coordinator = None
  }
  
}

/**
 * Launched BSP actor in forked process.
 * @param conf contains common setting for the forked process instead of tasks
 *             to be executed later on.
 */
// TODO: when coordinator finishes its execution. notify task counsellor and
//       update slot's task attempt id to none.
class Container(sys: String, slotSeq: Int, host: String, port: Int, 
                taskCounsellor: ActorRef) 
  extends LocalService with Computation {

  import Container._

  protected var setting = Setting.container(sys, slotSeq, host, port) 

  /**
   * Capture exceptions thrown by coordinator, etc.
   * Report to master and stop actors when necessary.
   *
   * InstantiationFailure will be restarted thrice. Coordinator will be stopped
   * if exceeds restart times. 
   * 
   * Offline function will stop container itself, and that will be captured by 
   * task counsellor and will be reported to master.
   */
  override val supervisorStrategy = 
    OneForOneStrategy(maxNrOfRetries = 3, withinTimeRange = 3 minutes) {
      case e: InstantiationFailure => {
        LOG.error("Fail to instantiate superstep because {}", e)
        Restart
      }
      case e: Exception => {  
        LOG.error("Unknown exception: {}", e)
        destroy
        Stop 
      }
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
  def doLaunch(task: Task) = initialize(setting, task, hamaHome, slotSeq, self)

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
   * Cancel the task that is running.
   * @param Receive is partial function.
   */
  def cancelTask: Receive = {
    case action: CancelTask => {
      doCancel(action.taskAttemptId)
      postCancel(slotSeq, action.taskAttemptId, sender)
    }
  }

  // TODO: any other operations?
  def doCancel(taskAttemptId: TaskAttemptID) = destroy 

  def postCancel(slotSeq: Int, taskAttemptId: TaskAttemptID, from: ActorRef) = 
    from ! new CancelAck(slotSeq, taskAttemptId)

  override def stopServices() = destroy

  protected def stopOperations: Receive = {
    case StopOperations(seq) => (seq == slotSeq) match { 
      case true => {
        context.unwatch(taskCounsellor)
        destroy
        stop
      }
      case false => LOG.error("Can't stop operation for slot seq {} expected, "+
                              " but {} found!", slotSeq, seq)
    }
  }

  protected def nameEquals(expected: Option[ActorRef], actual: ActorRef): 
    Boolean = expected.map { e => actual.path.name.equals(e.path.name) }.
                       getOrElse(false)

  /**
   * When {@link TaskCounsellor} is offline, {@link Container} will shutdown
   * itself.
   * @param target may be {@link TaskCounsellor}, {@link Coordinator}, etc.
   */
  override def offline(target: ActorRef) = 
    if(nameEquals(Option(taskCounsellor), target)) {
      LOG.error("{} is offline, Shutdown {}!", target.path.name, name)
      self ! ShutdownContainer
    } else if(nameEquals(tasklog, target) || nameEquals(messenger, target) ||
       nameEquals(peer, target) || nameEquals(coordinator, target)) {
      destroy
      stop  // TODO: replaced by reporting task failure to task counsellor?
    } else LOG.warning("Unexpected actor {} is offline!", target.path.name)

  protected def taskFinished: Receive = {
    case finished: TaskFinished => taskCounsellor ! finished 
  }

  protected def taskReport: Receive = {
    case taskReport: TaskReport => taskCounsellor ! taskReport 
  }

  override def receive = launchTask orElse resumeTask orElse cancelTask orElse stopOperations orElse superviseeOffline orElse taskFinished orElse taskReport orElse unknown

}
