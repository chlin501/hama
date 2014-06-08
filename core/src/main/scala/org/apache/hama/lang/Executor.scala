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
package org.apache.hama.lang 

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import akka.actor.Terminated
import akka.event.Logging
import java.io.BufferedReader
import java.io.BufferedWriter
import java.io.File
import java.io.FilenameFilter
import java.io.FileOutputStream
import java.io.FileWriter
import java.io.InputStream
import java.io.InputStreamReader
import java.io.IOException
import java.io.OutputStream
import java.lang.ProcessBuilder
import org.apache.commons.lang.math.NumberUtils
import org.apache.hadoop.io.IOUtils
import org.apache.hama.groom.BSPPeerContainer
import org.apache.hama.groom.ContainerReady
import org.apache.hama.groom.ContainerStopped
import org.apache.hama.groom.KillAck
import org.apache.hama.groom.KillTask
import org.apache.hama.groom.LaunchAck
import org.apache.hama.groom.LaunchTask
import org.apache.hama.groom.PullForExecution
import org.apache.hama.groom.ResumeAck
import org.apache.hama.groom.ResumeTask
import org.apache.hama.groom.StopContainer
import org.apache.hama.groom.ShutdownContainer
import org.apache.hama.HamaConfiguration
import org.apache.hama.fs.Operation
import org.apache.hama.util.BSPNetUtils
import scala.collection.immutable.Queue
import scala.collection.JavaConversions._

final case class Command(msg: Any, recipient: ActorRef)
final case object StreamClosed

trait TaskLog {

  def log(name: String, input: InputStream, conf: HamaConfiguration, 
          executor: ActorRef, ext: String, error: (String, Any*) => Unit) {
    import scala.language.postfixOps
    try { 
      val logPath = System.getProperty("hama.log.dir")
      logPath match {
        case null | "" => error("{} is not set!", "hama.log.dir")
        case _ => {
          if(!conf.getBoolean("bsp.tasks.log.console", false)) {
            val logDir = new File(logPath)
            if(!logDir.exists) logDir.mkdirs
            val out = new FileOutputStream(new File(logDir, 
              "%s.%s".format(executor.path.name, ext)))
            Iterator.continually(input.read).takeWhile(-1!=).foreach(out.write) 
          } else {
            Iterator.continually(input.read).takeWhile(-1!=).foreach(println) 
          }
        }
      }
    } catch { 
      case e: Exception => error("Fail reading "+name, e) 
    } finally { 
      input.close 
      executor ! StreamClosed 
    }
  }

}

class StdOut(input: InputStream, conf: HamaConfiguration, executor: ActorRef) 
      extends Actor with TaskLog { 

  val LOG = Logging(context.system, this)
  
  override def preStart = 
    log(self.path.name, input, conf, executor, "log", LOG.error)

  override def receive = { 
    case msg@_ => LOG.warning("Unknown stdout message {}", msg)
  }
}

class StdErr(input: InputStream, conf: HamaConfiguration, executor: ActorRef) 
      extends Actor with TaskLog {
  val LOG = Logging(context.system, this)

  override def preStart = 
    log(self.path.name, input, conf, executor, "err", LOG.error)

  override def receive = { 
    case msg@_ => LOG.warning("Unknown stderr message {}", msg)
  }
}

/**
 * An actor forks a child process for executing tasks.
 * @param conf cntains necessary setting for launching the child process.
 */
class Executor(conf: HamaConfiguration, taskManagerListener: ActorRef) 
      extends Actor {
 
  val LOG = Logging(context.system, this)
  val pathSeparator = System.getProperty("path.separator")
  val fileSeparator = System.getProperty("file.separator")
  val javaHome = System.getProperty("java.home")
  val hamaHome = System.getProperty("hama.home.dir")
  val javacp: String  = System.getProperty("java.class.path")
  val logPath: String = System.getProperty("hama.log.dir")
  val taskManagerName = conf.get("bsp.groom.taskmanager.name", "taskManager") 
  val operation = Operation.create(conf)
  var commandQueue = Queue[Command]()
  protected var bspPeerContainer: ActorRef =_
  protected var stdout: ActorRef = _
  protected var stderr: ActorRef = _
  protected var isStdoutClosed = false
  protected var isStderrClosed = false
  protected var process: Process = _
  protected var slotSeq: Int = -1

  /**
   * Pick up a port value configured in HamaConfiguration object. Otherwise
   * use default 50001.
   * @param String of the port value.
   */
  def taskPort: String = {
    val port = BSPNetUtils.getFreePort(50002).toString
    LOG.debug("Port value to be used is {}", port)
    port
  }

  // Bug: it seems using conf.get the stack lost track
  //      following execution e.g. LOG.info after conf.get disappers 
  def defaultOpts: String = conf.get("bsp.child.java.opts", "-Xmx200m")

  /**
   * Assemble java command for launching the child process.  
   * @param cp is the classpath.
   * @param slotSeq indicates to which slot this process belongs.
   * @param child is the class used to launced the process.
   * @return Seq[String] is the command for launching the process.
   */
  def javaArgs(cp: String, slotSeq: Int, child: Class[_]): Seq[String] = {
    val java = new File(new File(javaHome, "bin"), "java").getCanonicalPath
    LOG.debug("Java for slot seq {} is at {}", slotSeq, java)
    val opts = defaultOpts
    val bspClassName = child.getName
    val groomActorSystemName = conf.get("bsp.groom.actor-system.name", 
                                        "GroomSystem")
    val command = Seq(java) ++ Seq(opts) ++  
                  Seq("-classpath") ++ Seq(classpath(hamaHome, cp)) ++
                  Seq(bspClassName) ++ Seq(groomActorSystemName) ++ 
                  Seq(taskPort) ++ Seq(slotSeq.toString)
    LOG.debug("java args: {}", command.mkString(" "))
    command
  }

  /**
   * Collect jar files found under ${HAMA_HOME}/lib to form the classpath 
   * variable for the child process.
   * @param hamaHome is pointed to hama home.dir directory.
   * @return String of classpath value.
   */
  def classpath(hamaHome: String, parentClasspath: String): String = {
    if(null == hamaHome) 
      throw new RuntimeException("Variable hama.home.dir is not set!")
    var cp = "./:%s:%s/conf".format(parentClasspath, hamaHome)
    val lib = new File(hamaHome, "lib")
    lib.listFiles(new FilenameFilter {
      def accept(dir: File, name: String): Boolean = true
    }).foreach( jar => { cp += ":"+jar })
    LOG.debug("Classpath: {}", cp)
    cp
  }

  /**
   * Fork a child process based on command assembled.
   * @param slotSeq indicate which seq the slot is.
   */
  def fork(slotSeq: Int) {
    val containerClass = 
      conf.getClass("bsp.child.class", classOf[BSPPeerContainer])
    LOG.debug("Container class to be instantiated is {}", containerClass)
    val cmd = javaArgs(javacp, slotSeq, containerClass)
    createProcess(cmd, conf) 
  }

  /**
   * Configure working directory, either be configuration's key 
   * "bsp.working.dir" or file system's working directory.
   * @param conf will store working directory configuration.
   * @return String of working directory.
   */  
  def defaultWorkingDirectory(conf: HamaConfiguration): String = {
    var workDir = conf.get("bsp.working.dir")
    workDir match {
      case null => { 
        val fsDir = operation.getWorkingDirectory
        LOG.debug("Use file system's working directory {}", fsDir.toString)
        conf.set("bsp.working.dir", fsDir.toString)
        workDir = fsDir.toString
      }
      case _ => 
    } 
    LOG.debug("Working directory for slot {} is set to {}", slotSeq, workDir)
    workDir
  }

  /**
   * Fork a child process as container.
   * @param cmd is the command to excute the process.
   * @param conf contains related information for creating process.
   */
  def createProcess(cmd: Seq[String], conf: HamaConfiguration) {
    val builder = new ProcessBuilder(asJavaList(cmd))
    builder.directory(new File(defaultWorkingDirectory(conf)))
    try {
      process = builder.start
      stdout = context.actorOf(Props(classOf[StdOut], 
                                     process.getInputStream, 
                                     conf, 
                                     self),
                               "stdout%s".format(slotSeq)) 
      stderr = context.actorOf(Props(classOf[StdErr], 
                                     process.getErrorStream, 
                                     conf,
                                     self),
                               "stderr%s".format(slotSeq)) 
    } catch {
      case ioe: IOException => 
        LOG.error("Fail launching BSPPeerContainer process {}", ioe)
    }
  }

  /**
   * Create a container for executing tasks that will assign to it.
   * @return Receive partial function.
   */
  def fork: Receive = {
    case Fork(slotSeq) => {
      if(0 >= slotSeq) 
        throw new IllegalArgumentException("Invalid slotSeq: "+slotSeq)
      this.slotSeq = slotSeq
      fork(slotSeq) 
    }
  } 

  /**
   * Ask {@link BSPPeerContainer} to launch a task.
   * This should happens after {@link BSPPeerContainer} is ready.
   * @param Receive is partial function.
   */
  def launchTask: Receive = {
    case action: LaunchTask =>  bspPeerContainer ! new LaunchTask(action.task)
  }

  def launchAck: Receive = {
    case action: LaunchAck => 
      taskManagerListener ! new LaunchAck(action.slotSeq, action.taskAttemptId) 
  }

  /** 
   * Ask {@link BSPPeerContainer} to resume a specific task.
   * This should happens after {@link BSPPeerContainer} is ready.
   * @param Receive is partial function.
   */
  def resumeTask: Receive = {
    case action: ResumeTask => bspPeerContainer ! new ResumeTask(action.task)
  }

  def resumeAck: Receive = {
    case action: ResumeAck => 
      taskManagerListener ! new ResumeAck(action.slotSeq, action.taskAttemptId)
  }

  /**
   * Ask {@link BSPPeerContainer} to kill the task that is currently running.
   * This should happens after {@link BSPPeerContainer} is ready.
   * @param Receive is partial function.
   */
  def killTask: Receive = {
    case action: KillTask => 
      bspPeerContainer ! new KillTask(action.taskAttemptId)
  }

  def killAck: Receive = {
    case action: KillAck => 
      taskManagerListener ! new KillAck(action.slotSeq, action.taskAttemptId)
  }

  /**
   * Once the stream, including input and error stream, is closed, the system
   * will destroy process automatically.
   */
  def streamClosed: Receive = {
    case StreamClosed => {
      LOG.debug("{} notifies InputStream is closed!", sender.path.name)
      if(sender.path.name.equals("stdout%s".format(slotSeq))) { 
        isStdoutClosed = true
      } else if(sender.path.name.equals("stderr%s".format(slotSeq))) {
        isStderrClosed = true
      } else LOG.warning("Unknown sender ask for closing stream {}.", 
                         sender.path.name)
      if(isStdoutClosed && isStderrClosed) self ! StopProcess
    }
  }

  /**
   * BSPPeerContainer notify when it's in ready state.
   * @return Receive is partial function.
   */
  def containerReady: Receive = {
    case ContainerReady => {
      bspPeerContainer = sender
      while(!commandQueue.isEmpty) {
        val (cmd, rest) = commandQueue.dequeue
        bspPeerContainer ! cmd.msg
        commandQueue = rest  
      }
      afterContainerReady(taskManagerListener)
    }
  }

  def afterContainerReady(target: ActorRef) = target ! PullForExecution(slotSeq) 
  /**
   * Notify when BSPPeerContainer is stopped.
   * @return Receive is partial function.
   */
  def containerStopped: Receive = {
    case ContainerStopped => taskManagerListener ! ContainerStopped
  }

  /**
   * Send StopContainer message to shutdown BSPPeerContainer process.
   * @return Receive is partial function.
   */
  def stopProcess: Receive = {
    case StopProcess => {
      bspPeerContainer match {
        case null => 
          commandQueue = commandQueue.enqueue(Command(StopContainer, sender)) 
        case _ => bspPeerContainer ! StopContainer 
      }
    }
  }

  /**
   * Issue shutdown command to {@link BSPPeerContainer}, which shuts down the
   * child process.
   * @param Receive is partial function.
   */
  def shutdownContainer: Receive = {
    case ShutdownContainer => {
      bspPeerContainer match {
        case null => commandQueue = 
          commandQueue.enqueue(Command(ShutdownContainer, sender)) 
        case _ => {
          LOG.debug("Shutdown container {}", bspPeerContainer)
          bspPeerContainer ! ShutdownContainer 
        }
      }
    }
  }

  def unknown: Receive = {
    case msg@_=> LOG.warning("Unknown message {} for Executor", msg)
  }

  def terminated: Receive = {
    case Terminated(target) => LOG.warning("{} is offline.", target.path.name)
  }

  def receive = launchAck orElse resumeAck orElse killAck orElse launchTask orElse resumeTask orElse killTask orElse containerReady orElse fork orElse streamClosed orElse stopProcess orElse containerStopped orElse terminated orElse shutdownContainer orElse unknown
     
}

