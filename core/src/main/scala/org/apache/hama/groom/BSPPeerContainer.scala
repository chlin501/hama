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
import org.apache.hama.bsp.TaskAttemptID
import org.apache.hama.bsp.v2.Task
import org.apache.hama.HamaConfiguration
import org.apache.hama.LocalService
import org.apache.hama.ProxyInfo
import org.apache.hama.RemoteService
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration.DurationInt

final case class Args(port: Int, seq: Int, config: Config)

private final case class Initialize(taskAttemptId: String)
private final case class Info(message: String)
private final case class Debug(message: String)
private final case class Warning(message: String)
private final case class Error(message: String)
private final case class Close(taskAttemptId: String)

private[groom] final class TaskLogger extends Actor {

  private val log = Logging(context.system, this)
  private val hamaHome = System.getProperty("hama.home.dir")
  private var taskAttemptId: String = _
  private val dir = new File("%s/logs".format(hamaHome))
  private var stdout: FileWriter = _
  private var stderr: FileWriter = _
  val Append = true

  override def receive = {
    case Initialize(attemptId) => {
      taskAttemptId = attemptId
      if(!dir.exists) dir.mkdirs
      stdout = new FileWriter(new File(dir, taskAttemptId+".log"), Append)
      stderr = new FileWriter(new File(dir, taskAttemptId+".err"), Append)
    }
    case Info(msg) => stdout.write(msg+"\n")
    case Debug(msg) => stdout.write(msg+"\n")
    case Warning(msg) => stderr.write(msg+"\n")
    case Error(msg) => stderr.write(msg+"\n")
    case Close(attemptId) => {
      if(null != taskAttemptId && !taskAttemptId.isEmpty) {
        if(taskAttemptId.equals(attemptId)) {
           try { } finally { stdout.close; stderr.close }
        } else {
          log.error("Try closing task logging but id {} not matched "+
                    "current {}", attemptId, taskAttemptId)
        }
      } else log.warning("Attempt to close log for task {} but {} not "+
                         "matched.", taskAttemptId, attemptId)
    }
    case msg@_ => log.warning("Unknown message "+msg+" for taksAttemptId "+
                              taskAttemptId)
  }
}

private[groom] trait Logger {

  def info(message: String, args: Any*) 

  def debug(message: String, args: Any*) 

  def warning(message: String, args: Any*) 

  def error(message: String, args: Any*) 
}

private[groom] class DefaultLogger(logger: ActorRef) extends Logger {

  protected[groom] def initialize(taskAttemptId: String) = if(null != logger) {
    if(null != taskAttemptId && !taskAttemptId.isEmpty) 
      logger ! Initialize(taskAttemptId)
    else throw new IllegalArgumentException("TaskAttemptId not provided to "+
                                            "intialize logging.")
  }

  def info(message: String, args: Any*) = if(null != logger) {
    logger ! Info(format(message, args))
  }

  def debug(message: String, args: Any*) = if(null != logger) {
    logger ! Debug(format(message, args))
  }

  def warning(message: String, args: Any*) = if(null != logger) {
    logger ! Warning(format(message, args))
  }

  def error(message: String, args: Any*) = if(null != logger) {
    logger ! Error(format(message, args))
  }

  protected[groom] def close(taskAttemptId: String) = if(null != logger) {
    if(null != taskAttemptId && !taskAttemptId.isEmpty) 
      logger ! Close(taskAttemptId)
    else throw new IllegalArgumentException("TaskAttemptId not provided to "+
                                            "close logging.")
  }

  /**
   * Replace place hold with arguments.
   * @param msg contains place holder.
   * @param args are values used to  
   */
  private def format(msg: String, args: Any*): String = {
    "" // TODO:
  }
 
}

private[groom] object TaskLogging {
 
  def apply(cxt: ActorContext, slotSeq: Int): Logger = {
    if(null == cxt) 
      throw new IllegalArgumentException("ActorContext is missing!")
    if(0 >= slotSeq)
      throw new IllegalArgumentException("Slot seq should be larger than 0.")
    val logger = cxt.actorOf(Props(classOf[TaskLogger]), 
                             "taskLogger%s".format(slotSeq))
    new DefaultLogger(logger)
  }
 
}

object BSPPeerContainer {

  def toConfig(port: Int): Config = {
    ConfigFactory.parseString(s"""
      peer {
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
              hostname = "127.0.0.1" 
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
    val port = args(0).toInt
    val seq = args(1).toInt
    val config = toConfig(port) 
    Args(port, seq, config)
  }

  def initialize(args: Array[String]): (ActorSystem, HamaConfiguration, Int) = {
    val defaultConf = new HamaConfiguration()
    val arguments = toArgs(args)
    val system = ActorSystem("BSPPeerSystem%s".format(arguments.seq), 
                             arguments.config.getConfig("peer"))
    defaultConf.setInt("bsp.child.slot.seq", arguments.seq)
    (system, defaultConf, arguments.seq)
  }

  def launch(system: ActorSystem, conf: HamaConfiguration, seq: Int) {
    system.actorOf(Props(classOf[BSPPeerContainer], conf), 
                   "bspPeerContainer%s".format(seq))
  }

  @throws(classOf[Throwable])
  def main(args: Array[String]) = {
    val (sys, conf, seq)= initialize(args)
    launch(sys, conf, seq)
  }
}

/**
 * Launched BSP actor in forked process.
 * @param conf contains setting sepcific to this service.
 */
class BSPPeerContainer(conf: HamaConfiguration) extends LocalService 
                                                with RemoteService {
 
  val logger = TaskLogging(context, slotSeq)

  val groomName = configuration.get("bsp.groom.name", "groomServer")
  protected var executor: ActorRef = _
  override def configuration: HamaConfiguration = conf
  def executorName: String = groomName+"_executor_"+slotSeq
  def slotSeq: Int = configuration.getInt("bsp.child.slot.seq", 1)

  // TODO: refactor for proxy lookup by test!
  protected def executorInfo: ProxyInfo = { 
     new ProxyInfo.Builder().withConfiguration(configuration).
                             withActorName(executorName).
                             appendRootPath(groomName). 
                             appendChildPath(executorName). 
                             buildProxyAtGroom
  }

  protected def executorPath: String = executorInfo.getPath

  override def name: String = "bspPeerContainer%s".format(slotSeq)
 
  override def initializeServices {
    lookup(executorName, executorPath)
  }

  override def afterLinked(proxy: ActorRef) {
    executor = proxy
    executor ! ContainerReady
    LOG.info("Slot seq {} sends ContainerReady to {}", 
              slotSeq, executor.path.name)
  }

  /**
   * Start executing task dispatched to the container.
   * @return Receive is partial function.
   */
  def processTask: Receive = {
    case task: Task => {
      LOG.info("Start processing task {}", task.getId)
      // not yet implemented ... 
      // initialize bsp peer interface
      // add logger variable
      // initializze logger (logger ! Initialize(task.getId))
      // load jar into actor  // need security mechanism
      // perform bsp() w/ another actor
    }
  }

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

  def shutdownSystem: Receive = {
    case ShutdownSystem => {
      LOG.info("Completely shutdown BSPContainer system ...")
      context.system.shutdown
    }
  }

  override def offline(target: ActorRef) {
    LOG.info("{} is unwatched.", target.path.name)
  }

  override def receive = shutdownSystem orElse stopContainer orElse processTask orElse isProxyReady orElse timeout orElse superviseeIsTerminated orElse unknown
}
