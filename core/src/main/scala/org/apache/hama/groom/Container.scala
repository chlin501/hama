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
import org.apache.hama.bsp.TaskAttemptID
import org.apache.hama.bsp.v2.ConfigureFor
import org.apache.hama.bsp.v2.Execute
import org.apache.hama.bsp.v2.Occupied
import org.apache.hama.bsp.v2.Task
import org.apache.hama.bsp.v2.TaskWorker
import org.apache.hama.HamaConfiguration
import org.apache.hama.logging.TaskLogger
import org.apache.hama.LocalService
import org.apache.hama.message.PeerMessenger
import org.apache.hama.monitor.Report
import org.apache.hama.monitor.TaskStat
import org.apache.hama.RemoteService
import org.apache.hama.sync.SyncException
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

object Container {

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
                                         "Container is forked.")
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
    launch(sys, classOf[Container], conf, seq)
  }
}

/**
 * Launched BSP actor in forked process.
 * Container is respoinsible for TaskWorker execution. So the relation between 
 * Container and TaskWorker is 1 on 1.
 * @param conf contains common setting for the forked process instead of tasks
 *             to be executed later on.
 */
class Container(conf: HamaConfiguration) extends LocalService 
                                         with RemoteService 
                                         with ActorLocator {

  import Container._

  // N.B.: When sending messages fails, Terminated message should be observed at
  //       PeerMessenger because we send messages through peer messenger.
  //       If remote peer messenger throws exception, it should be able to 
  //       receives/ replies messages as usual.
  //       So we should watch peer messenger and if remote peer messenger is 
  //       terminated (offline); if remote terminated, notify container in 
  //       reacting for such event.

  // check what exceptions are thrown in superstep bsp, worker, coordinator.
  // remove unnecessary exceptions or replace exceptions with notifying 
  // container for reaction!
  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 3, withinTimeRange = 1 minute) {
       //case _: IllegalArgumentException => Stop
       case _: IOException => Restart // this may be user io issue. 
       case _: SyncException => Resume  
    }

  /**
   * This serves as internal communicator between peers.
   */
  protected val peerMessenger = createPeerMessenger(identifier(conf))

  /**
   * A log class for tasks to be executed. Sending TaskAttemptID to switch
   * for logging to different directory. 
   */
  protected val tasklog = createTaskLogger[TaskLogger](classOf[TaskLogger], 
                                                       getLogDir(hamaHome), 
                                                       slotSeq) 

  protected var executor: Option[ActorRef] = None

  protected var taskWorker: Option[ActorRef] = None

  /**
   * The stat to a specific task. Used to check the execution of a task and
   * related operation.
   */
  protected var taskStat: Option[TaskStat] = None

  override def configuration: HamaConfiguration = conf

  // TODO: check if any better way to config hama home.
  protected def hamaHome: String = System.getProperty("hama.home.dir")

  protected def getLogDir(hamaHome: String): String = hamaHome+tasklogsPath

  protected def slotSeq: Int = configuration.getInt("bsp.child.slot.seq", 1)

  protected def executorName: String = "groomServer_executor_"+slotSeq
 
  override def initializeServices =
    lookup(executorName, locate(ExecutorLocator(configuration)))

  override def afterLinked(proxy: ActorRef) {
    executor = Option(proxy)
    executor.map( found => {
      found ! ContainerReady
      LOG.info("Slot seq {} sends ContainerReady to {}", slotSeq, 
               found.path.name)
    })
  }

  protected def createPeerMessenger(id: String): ActorRef = 
    spawn("peerMessenger_"+id, classOf[PeerMessenger], conf)

  /**
   * Check if the task worker is running. true if a worker is running; false 
   * otherwise.
   * @param worker wraps the task worker as option.
   * @return Boolean denotes worker is running if true; otherwise false.
   */
  protected def isOccupied(worker: Option[ActorRef]): Boolean = 
    worker.map( v => true).getOrElse(false)

  /**
   * - Asynchronouly create task with an actor.
   * - When that actor finishes setup, sending ack back to executor.
   * @param Receive is partial function.
   */
  def launchTask: Receive = {
    case action: LaunchTask => if(!isOccupied(taskWorker)) {
      doLaunch(action.task)
      postLaunch(slotSeq, action.task.getId, sender)
    } else sender ! new Occupied(slotSeq, action.task.getId) 
  }

  protected def identifier(conf: HamaConfiguration): String = 
    conf.getInt("bsp.child.slot.seq", -1) match {
      case -1 => throw new RuntimeException("Slot seq shouldn't be -1!")
      case seq@_ => {
        val host = conf.get("bsp.peer.hostname",
                            InetAddress.getLocalHost.getHostName)
        val port = conf.getInt("bsp.peer.port", 61000)
        "BSPPeerSystem%d@%s:%d".format(seq, host, port)
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
    taskWorker = Option(spawn("taskWoker", classOf[TaskWorker], configuration, 
                              self, peerMessenger, tasklog)).map( worker => {
      context.watch(worker)
      worker ! ConfigureFor(task)
      worker ! Execute(task.getId.toString, configuration, 
                       task.getConfiguration)
      worker
    })
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
    case action: ResumeTask => if(!isOccupied(taskWorker)) {
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
      executor.map( found => found ! ContainerStopped)
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
      executor.map( found => context.unwatch(found) )
      //LOG.debug("Stop {} itself ...", name)
      //context.stop(self)
      LOG.info("Completely shutdown BSPContainer system ...")
      context.system.shutdown
    }
  }

  /**
   * When {@link Executor} is offline, {@link Container} will shutdown
   * itself.
   * @param target actor is {@link Executor}
   */
  override def offline(target: ActorRef) {
    LOG.info("{} is offline!", target.path.name)
    val ExecutorName = executorName
    target.path.name match {
      case "taskWorker" => // TODO: mark task as fail (update taskStat), restart, etc.
      case `ExecutorName` => self ! ShutdownContainer
      case unexpected@_ => 
        LOG.warning("Unexpected actor {} is offline!", unexpected)
    }
  }

  /**
   * Report running worker's task stat data.
   * @return Receive is partial function.
   */
  def reportStat: Receive = {
    case Report(stat) => updateStat(stat)
  }

  /**
   * Update task stat data.
   * @param stat contains the Task currently executed.
   */
  protected def updateStat(stat: TaskStat) = taskStat = Option(stat)

  override def receive = reportStat orElse launchTask orElse resumeTask orElse killTask orElse shutdownContainer orElse stopContainer orElse actorReply orElse timeout orElse superviseeIsTerminated orElse unknown
}
