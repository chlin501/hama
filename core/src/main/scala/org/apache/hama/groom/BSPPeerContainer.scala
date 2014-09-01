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
import org.apache.hama.bsp.v2.ConfigureFor
import org.apache.hama.bsp.v2.Execute
import org.apache.hama.bsp.v2.Task
import org.apache.hama.bsp.v2.Worker
import org.apache.hama.HamaConfiguration
import org.apache.hama.logging.TaskLogger
import org.apache.hama.LocalService
import org.apache.hama.message.PeerMessenger
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

object BSPPeerContainer {

  val tasklogsPath = "/logs/taskslogs"

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

  import BSPPeerContainer._

  /**
   * This serves as internal communicator between peers.
   */
  protected val peerMessenger: ActorRef = createPeerMessenger(identifier(conf))

  /**
   * A log class for tasks to be executed. Sending TaskAttemptID to switch
   * for logging to different directory. 
   */
  protected val tasklog = createTaskLogger[TaskLogger](classOf[TaskLogger], 
                                                       getLogDir(hamaHome), 
                                                       slotSeq) 

  protected var executor: Option[ActorRef] = None

  override def configuration: HamaConfiguration = conf

  // TODO: check if any better way to set hama home.
  protected def hamaHome: String = System.getProperty("hama.home.dir")

  protected def getLogDir(hamaHome: String): String = hamaHome+tasklogsPath

  protected def slotSeq: Int = configuration.getInt("bsp.child.slot.seq", 1)

  protected def executorName: String = "groomServer_executor_"+slotSeq
 
  override def initializeServices {
    lookup(executorName, locate(ExecutorLocator(configuration)))
  }

  override def afterLinked(proxy: ActorRef) {
    executor = Some(proxy)
    executor match {
      case Some(found) => {
        found ! ContainerReady
        LOG.info("Slot seq {} sends ContainerReady to {}", 
                 slotSeq, found.path.name)
      }
      case None => 
    }
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

  protected def createPeerMessenger(id: String): ActorRef = 
    spawn("peerMessenger_"+id, classOf[PeerMessenger], conf)

  protected def identifier(conf: HamaConfiguration): String = {
    conf.getInt("bsp.child.slot.seq", -1) match {
      case -1 => throw new RuntimeException("Slot seq shouldn't be -1!")
      case seq@_ => {
        val host = conf.get("bsp.peer.hostname",
                            InetAddress.getLocalHost.getHostName)
        val port = conf.getInt("bsp.peer.port", 61000)
        "BSPPeerSystem%d@%s:%d".format(seq, host, port)
      }
    }
  }

  protected def createTaskLogger[A <: TaskLogger](logger: Class[A], 
                                                  logDir: String,
                                                  seq: Int): ActorRef = 
    spawn("taskLogger%s".format(seq), logger, logDir)

  /**
   * Start executing the task in another actor.
   * @param task that is supplied to be executed.
   */
  def doLaunch(task: Task) { 
    val taskWorker = spawn("taskWoker", classOf[Worker], configuration, self, 
                       peerMessenger, tasklog)
    context.watch(taskWorker)
    taskWorker ! ConfigureFor(task)
    taskWorker ! Execute(task.getId.toString, configuration, 
                         task.getConfiguration)
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
      executor match {
        case Some(found) => found ! ContainerStopped  
        case None =>
      }
      LOG.debug("ContainerStopped message is sent ...")
    }
  }

  /**
   * Shutdown the system. The spawned process will be stopped as well.
   * @return Receive is partial function.
   */
  def shutdownContainer: Receive = {
    case ShutdownContainer => {
      LOG.debug("Unwatch remote executro {} ...", executor)
      executor match {
        case Some(found) => context.unwatch(found) 
        case None =>
      }
      //LOG.debug("Stop {} itself ...", name)
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

  override def receive = launchTask orElse resumeTask orElse killTask orElse shutdownContainer orElse stopContainer orElse actorReply orElse timeout orElse superviseeIsTerminated orElse unknown
}
