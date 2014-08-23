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

import akka.actor.ActorContext
import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Cancellable
import akka.event.Logging
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import java.io.File
import java.io.FileWriter
import java.net.InetAddress
import org.apache.hama.bsp.TaskAttemptID
import org.apache.hama.bsp.v2.Bind
import org.apache.hama.bsp.v2.ConfigureFor
import org.apache.hama.bsp.v2.Execute
import org.apache.hama.bsp.v2.Task
import org.apache.hama.bsp.v2.Worker
import org.apache.hama.HamaConfiguration
import org.apache.hama.LocalService
import org.apache.hama.RemoteService
import org.apache.hama.util.ActorLocator
import org.apache.hama.util.ExecutorLocator
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration.DurationInt

/**
 * Args contains information for launching akka ActorSystem.
 * Note that listeningTo value may be bound to 0.0.0.0 so that the system can
 * accept messages from the machine with many interfaces bound to it.  
 * 
 * @param actorSystemName denotes the actor system that runs actors on forked.
 *                        process.
 * @param listeningTo denotes the host/ interfaces the remote actor modules 
 *                    listens to.
 * @param port is the port value used by the remote actor module.
 * @param seq tells this process is the N-th forked by {@link TaskManager}.
 * @param config contains related setting for activating remote actor module.
 */
final case class Args(actorSystemName: String, listeningTo: String, port: Int, 
                      seq: Int, config: Config)

/**
private final case class Initialize(taskAttemptId: TaskAttemptID)
 * Following log levels are only intended for container use.
private final case class Info(message: String)
private final case class Debug(message: String)
private final case class Warning(message: String)
private final case class Error(message: String)
private final case class Close(taskAttemptId: TaskAttemptID)

private[groom] final class TaskLogger(conf: HamaConfiguration) extends Actor {

  private val log = Logging(context.system, this)
  private val hamaHome = System.getProperty("hama.home.dir")
  private var taskAttemptId: TaskAttemptID = _
  private val logDir = new File(conf.get("bsp.child.log.dir", 
                                hamaHome+"/logs/tasklogs"))
  private var stdout: FileWriter = _
  private var stderr: FileWriter = _
  val Append = true

  override def receive = {
    case Initialize(attemptId) => {
      taskAttemptId = attemptId
      val jobId = taskAttemptId.getJobID.toString
      val jobIdDir = new File(logDir, jobId)
      if(!jobIdDir.exists) jobIdDir.mkdirs
      stdout = new FileWriter(new File(jobIdDir, taskAttemptId.toString+".log"),
                              Append)
      stderr = new FileWriter(new File(jobIdDir, taskAttemptId.toString+".err"),
                              Append)
    }
    case Info(msg) => stdout.write(msg+"\n")
    case Debug(msg) => stdout.write(msg+"\n")
    case Warning(msg) => stderr.write(msg+"\n")
    case Error(msg) => stderr.write(msg+"\n")
    case Close(attemptId) => {
      if(null != taskAttemptId) {
        if(taskAttemptId.equals(attemptId)) {
           try { } finally { stdout.close; stderr.close }
        } else {
          log.error("Try closing task logging but id {} not matched "+
                    "current {}", attemptId.toString, taskAttemptId.toString)
        }
      } else log.warning("Attempt to close log for task {} but {} not "+
                         "matched.", taskAttemptId.toString, attemptId.toString)
    }
    case msg@_ => log.warning("Unknown message "+msg+" for taksAttemptId "+
                              taskAttemptId.toString)
  }
}

private[groom] trait Logger { 

  def info(message: String, args: Any*) 

  def debug(message: String, args: Any*) 

  def warning(message: String, args: Any*) 

  def error(message: String, args: Any*) 
}

private[groom] class DefaultLogger(logger: ActorRef) extends Logger {

  protected[groom] def initialize(taskAttemptId: TaskAttemptID) = 
    if(null != logger) {
      if(null != taskAttemptId) logger ! Initialize(taskAttemptId)
      else throw new IllegalArgumentException("TaskAttemptId not provided to "+
                                              "intialize logging.")
    }

  override def info(message: String, args: Any*) = if(null != logger) {
    logger ! Info(format(message, args))
  }

  override def debug(message: String, args: Any*) = if(null != logger) {
    logger ! Debug(format(message, args))
  }

  override def warning(message: String, args: Any*) = if(null != logger) {
    logger ! Warning(format(message, args))
  }

  override def error(message: String, args: Any*) = if(null != logger) {
    logger ! Error(format(message, args))
  }

  protected[groom] def close(taskAttemptId: TaskAttemptID) = 
    if(null != logger) {
      if(null != taskAttemptId) logger ! Close(taskAttemptId)
      else throw new IllegalArgumentException("TaskAttemptId not provided to "+
                                              "close logging.")
    }

   * Format string.
   * @param msg contains place holder to be formatted.
   * @param args are values used to  
  private def format(msg: String, args: Any*): String = 
    msg.replace("{}", "%s").format(args:_*)
 
}

private[groom] object TaskLogging {
 
  def apply(cxt: ActorContext, conf: HamaConfiguration, slotSeq: Int): 
      Logger = {
    if(null == cxt) 
      throw new IllegalArgumentException("ActorContext is missing!")
    if(null == conf)
      throw new IllegalArgumentException("HamaConfiguration not provided!")
    if(0 >= slotSeq)
      throw new IllegalArgumentException("Slot seq should be larger than 0.")
    val logger = cxt.actorOf(Props(classOf[TaskLogger], conf), 
                             "taskLogger%s".format(slotSeq))
    new DefaultLogger(logger)
  }
 
}
 */

object BSPPeerContainer {

  // TODO: all config need to post to zk
  def toConfig(listeningTo: String, port: Int): Config = {
    ConfigFactory.parseString(s"""
      peerContainer {
        akka {
          actor {
            provider = "akka.remote.RemoteActorRefProvider"
            serializers {
              java = "akka.serialization.JavaSerializer"
              proto = "akka.remote.serialization.ProtobufSerializer"
              writable = "org.apache.hama.io.serialization.WritableSerializer"
            }
            serialization-bindings {
              "com.google.protobuf.Message" = proto
              "org.apache.hadoop.io.Writable" = writable
            }
          }
          remote {
            netty.tcp {
              hostname = "$listeningTo"
              port = $port 
            }
          }
        }
      }
    """)
  }

  def toArgs(args: Array[String]): Args = {
    if(null == args || 0 == args.length)
      throw new IllegalArgumentException("No arguments supplied when "+
                                         "BSPPeerContainer is forked.")
    val actorSystemName = args(0)
    // N.B.: it may binds to 0.0.0.0 for all inet.
    val listeningTo = args(1) 
    val port = args(2).toInt
    val seq = args(3).toInt
    val config = toConfig(listeningTo, port) 
    Args(actorSystemName, listeningTo, port, seq, config)
  }

  def initialize(args: Array[String]): (ActorSystem, HamaConfiguration, Int) = {
    val defaultConf = new HamaConfiguration()
    val arguments = toArgs(args)
    val system = ActorSystem("BSPPeerSystem%s".format(arguments.seq), 
                             arguments.config.getConfig("peerContainer"))
    defaultConf.set("bsp.groom.actor-system.name", arguments.actorSystemName)
    val listeningTo = defaultConf.get("bsp.peer.hostname", 
                                      arguments.listeningTo)
    // N.B.: if default listening to 0.0.0.0, change host to host name.
    if("0.0.0.0".equals(listeningTo)) { 
       defaultConf.set("bsp.peer.hostname", 
                       InetAddress.getLocalHost.getHostName)
    }
    defaultConf.setInt("bsp.peer.port", arguments.port)
    defaultConf.setInt("bsp.child.slot.seq", arguments.seq)
    (system, defaultConf, arguments.seq)
  }

  def launch(system: ActorSystem, containerClass: Class[_], 
             conf: HamaConfiguration, seq: Int) {
    system.actorOf(Props(containerClass, conf), 
                   "bspPeerContainer%s".format(seq))
  }

  @throws(classOf[Throwable])
  def main(args: Array[String]) = {
    val (sys, conf, seq)= initialize(args)
    launch(sys, classOf[BSPPeerContainer], conf, seq)
  }
}

/**
 * Launched BSP actor in forked process.
 * @param conf contains common setting for the forked process instead of tasks
 *             to be executed later on.
 */
class BSPPeerContainer(conf: HamaConfiguration) extends LocalService 
                                                with RemoteService 
                                                with ActorLocator {

  protected var executor: ActorRef = _

  // TODO: read from zk?
  val groomName = configuration.get("bsp.groom.name", "groomServer")

  override def configuration: HamaConfiguration = conf

  def slotSeq: Int = configuration.getInt("bsp.child.slot.seq", 1)

  def executorName: String = groomName+"_executor_"+slotSeq

  override def name: String = "bspPeerContainer%s".format(slotSeq)
 
  override def initializeServices {
    lookup(executorName, locate(ExecutorLocator(configuration)))
  }

  override def afterLinked(proxy: ActorRef) {
    executor = proxy
    executor ! ContainerReady
    LOG.info("Slot seq {} sends ContainerReady to {}", 
              slotSeq, executor.path.name)
  }

  /**
   * - Asynchronouly create task with an actor.
   * - When that actor finishes setup, sending ack back to executor.
   * @param Receive is partial function.
   */
  def launchTask: Receive = {
    case action: LaunchTask => {
      doLaunch(action.task)
      postLaunch(slotSeq, action.task.getId, sender)
    }
  }

  /**
   * Start executing the task in another actor.
   * @param task that is supplied to be executed.
   */
  def doLaunch(task: Task) { 
    val worker = context.actorOf(Props(classOf[Worker]))
    context.watch(worker)
    worker ! Bind(configuration, context.system) 
    worker ! ConfigureFor(task)
    worker ! Execute(task.getId.toString, configuration, task.getConfiguration)
  }

  def postLaunch(slotSeq: Int, taskAttemptId: TaskAttemptID, from: ActorRef) = {
    from ! new LaunchAck(slotSeq, taskAttemptId)
    LOG.debug("LaunchAck is sent back!")
  }

  /**
   * - Asynchronouly create task with an actor.
   * - When that actor finishes setup, sending ack back to executor.
   * @param Receive is partial function.
   */
  def resumeTask: Receive = {
    case action: ResumeTask => {
      doResume(action.task)
      postResume(slotSeq, action.task.getId, sender)
    }
  }

  def doResume(task: Task) {
    LOG.info("function doResume is not yet implemented!") // TODO:
  }

  def postResume(slotSeq: Int, taskAttemptId: TaskAttemptID, from: ActorRef) = {
    from ! new ResumeAck(slotSeq, taskAttemptId)
    LOG.debug("ResumeAck is sent back!")
  }
    

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

  def doKill(taskAttemptId: TaskAttemptID) {
    LOG.info("function doKill is not yet implemented!") // TODO:
  }

  def postKill(slotSeq: Int, taskAttemptId: TaskAttemptID, from: ActorRef) = 
    from ! new KillAck(slotSeq, taskAttemptId)

  /**
   * A function to close all necessary operations before shutting down the 
   * system.
   */
  protected def close { 
    LOG.info("Stop related operations before exiting programme ...")
  }

  override def postStop {
    close
    super.postStop
  }

  /**
   * Close all related process operations and then shutdown the actor system.
   * @return Receive is partial function.
   */
  def stopContainer: Receive = {
   case StopContainer => {
      executor ! ContainerStopped  
      LOG.info("Send ContainerStopped message ...")
    }
  }

  /**
   * Shutdown the system. The spawned process will be stopped as well.
   * @return Receive is partial function.
   */
  def shutdownContainer: Receive = {
    case ShutdownContainer => {
      LOG.debug("Unwatch remote executro {} ...", executor)
      context.unwatch(executor) 
      //LOG.debug("Stop {} itself ...", self.path.name)
      //context.stop(self)
      LOG.info("Completely shutdown BSPContainer system ...")
      context.system.shutdown
    }
  }

  /**
   * When {@link Executor} is offline, {@link BSPPeerContainer} will shutdown
   * itself.
   * @param target actor is {@link Executor}
   */
  override def offline(target: ActorRef) {
    LOG.info("{} is offline. So we are going to shutdown itself ...", target)
    self ! ShutdownContainer
  }

  override def receive = launchTask orElse resumeTask orElse killTask orElse shutdownContainer orElse stopContainer /*orElse processTask*/ orElse isProxyReady orElse timeout orElse superviseeIsTerminated orElse unknown
}
