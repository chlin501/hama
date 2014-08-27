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

import akka.actor.Actor
import akka.actor.ActorContext
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.event.LoggingAdapter
import java.io.File
import java.net.InetAddress
import java.net.URL
import java.net.URLClassLoader
import org.apache.hadoop.fs.Path
import org.apache.hama.Agent
import org.apache.hama.bsp.TaskAttemptID
import org.apache.hama.fs.Operation
import org.apache.hama.HamaConfiguration
import org.apache.hama.logging.TaskLog
import org.apache.hama.logging.TaskLogParam
import org.apache.hama.logging.TaskLogParameter
import org.apache.hama.message.PeerMessenger

sealed trait WorkerOperation
final case class Bind(conf: HamaConfiguration, 
                      actorSystem: ActorSystem) extends WorkerOperation
final case class ConfigureFor(conf: HamaConfiguration,
                              task: Task) extends WorkerOperation
final case class Execute(taskAttemptId: String,
                         conf: HamaConfiguration, 
                         taskConf: HamaConfiguration) extends WorkerOperation
final case class Close extends WorkerOperation

object Worker {

  val tasklogsPath = "/logs/taskslogs"

}

/**
 * This is the actual class that perform {@link Superstep}s execution.
 */
protected[v2] class Worker(container: ActorRef) extends SuperstepBSP 
                                                with Agent 
                                                with TaskLog 
                                                with TaskLogParameter {

  import Worker._ 

  protected val peer = new Coordinator
 
  protected var task: Option[Task] = None

  protected var slotSeq: Int = -1 

  override def getTaskLogParam(): TaskLogParam = 
    TaskLogParam(context.system, getLogDir(hamaHome), slotSeq) 

  // TODO: check if any better way to set hama home. 
  protected def hamaHome: String = System.getProperty("hama.home.dir")

  protected def getLogDir(hamaHome: String): String = hamaHome+tasklogsPath

  override def getTask(): Task = task match {
    case None => throw new NullPointerException("Task is not yet ready!")
    case Some(found) => found
  }

  /**
   * This ties coordinator to a particular task.
   */
  def configureFor: Receive = {
    case ConfigureFor(conf, task) => {
      
      setConf(task.getConfiguration)
      LOG.info("Configure this worker to task attempt id {}", 
               task.getId.toString)
      val peerMessenger = createPeerMessenger(conf)
      LOG.info("Peer messenger {} is created! ", peerMessenger.path.name)
      configureFor(conf, task, peerMessenger)
    }
  }

  protected def createPeerMessenger(conf: HamaConfiguration): ActorRef = {
    val id = identifier(conf) 
    context.actorOf(Props(classOf[PeerMessenger]), "peerMessenger_"+id)
  }

  protected def identifier(conf: HamaConfiguration): String = {
    conf.getInt("bsp.child.slot.seq", -1) match {
      case -1 => throw new RuntimeException("Slot seq is -1!") 
      case seq@_ => {
        val host = conf.get("bsp.peer.hostname",
                            InetAddress.getLocalHost.getHostName)
        val port = conf.getInt("bsp.peer.port", 61000)
        "BSPPeerSystem%d@%s:%d".format(seq, host, port)
      }
    }
  }


  /**
   * Start executing {@link Superstep}s accordingly.
   * @return Receive id partial function.
   */
  def execute: Receive = {
    case Execute(taskAttemptId, conf, taskConf) => 
      doExecute(taskAttemptId, conf, taskConf)
  }

  /**
   * Execute supersteps according to the task configuration provided.
   * @param taskConf is HamaConfiguration specific to a pariticular task.
   */
  protected def doExecute(taskAttemptId: String, 
                          conf: HamaConfiguration, 
                          taskConf: HamaConfiguration) {
    addJarToClasspath(taskAttemptId, taskConf)
    setup(peer)
    bsp(peer)
  }

  /**
   * Dynamically add client jar url to the {@link URLClassLoader}, which will
   * be used for intializing necessary user customized classes. 
   * @param taskConf is the configuration sepcific to a task.
   * @return Option[ClassLoader] contains class loader with client jar url.
   */
  def addJarToClasspath(taskAttemptId: String, 
                        taskConf: HamaConfiguration): Option[ClassLoader] = {
    val jar = taskConf.get("bsp.jar")
    LOG.info("Jar path found in task configuration is {}", jar)
    jar match {
      case null|"" => None
      case remoteUrl@_ => {
        val operation = Operation.get(taskConf)
        val localJarPath = createLocalPath(taskAttemptId, taskConf, operation) 
        operation.copyToLocal(new Path(remoteUrl))(new Path(localJarPath))
        LOG.info("remote file {} is copied to {}", remoteUrl, localJarPath) 
        val url = normalizePath(localJarPath)
        val loader = Thread.currentThread.getContextClassLoader
        val newLoader = new URLClassLoader(Array[URL](url), loader) 
        taskConf.setClassLoader(newLoader) 
        LOG.info("User jar {} is added to the newly created url class loader "+
                 "for job {}", url, taskAttemptId)
        Some(newLoader)   
      }
    }
  }

  /**
   * Normoalize user jar path to URL.
   * @param jarPath is the path pointed to the jar downloaded from remote 
   *                repository.
   * @return URL translated from the jar path string.
   */
  protected def normalizePath(jarPath: String): URL = 
    new File(jarPath).toURI.toURL

  /**
   * Create path as dest for the jar to be downloaded based on 
   * TaskAttemptID.
   */
  def createLocalPath(taskAttemptId: String, 
                      config: HamaConfiguration,
                      operation: Operation): String = {
    val localDir = config.get("bsp.local.dir", "/tmp/bsp/local")
    val subDir = config.get("bsp.local.dir.sub_dir", "bspmaster")
    if(!operation.local.exists(new Path(localDir, subDir)))
      operation.local.mkdirs(new Path(localDir, subDir))
    "%s/%s/%s.jar".format(localDir, subDir, taskAttemptId.toString)
  }

  /**
   * Close underlying {@link BSPPeer} operations.
   * @return Receive is partial function.
   */
  def close: Receive = {
    case Close => close
  }

  override def receive = configureFor orElse execute orElse close orElse unknown
}
