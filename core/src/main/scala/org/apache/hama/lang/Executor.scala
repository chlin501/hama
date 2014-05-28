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
import org.apache.hama.groom.PullForExecution
import org.apache.hama.groom.StopContainer
import org.apache.hama.groom.ShutdownSystem
import org.apache.hama.HamaConfiguration
import org.apache.hama.fs.Operation
import org.apache.hama.util.BSPNetUtils
import scala.collection.JavaConversions._

final case class Fork(slotSeq: Int, conf: HamaConfiguration)
final case object StreamClosed
final case object StopProcess

class StdOut(input: InputStream, executor: ActorRef) extends Actor { 
  import scala.language.postfixOps
  val LOG = Logging(context.system, this)
  override def preStart {
    try { 
      val hamaHome = System.getProperty("hama.home.dir")
      val dir = new File("%s/logs".format(hamaHome))
      if(!dir.exists) dir.mkdirs
      val out = 
        new FileOutputStream(new File(dir, "%s.log".format(executor.path.name)))
      Iterator.continually(input.read).takeWhile(-1!=).foreach(out.write) 
    } catch { 
      case e: Exception => LOG.error("Fail reading stdout {}.", e) 
    } finally { 
      input.close 
      executor ! StreamClosed 
    }
  }

  override def receive = { 
    case msg@_ => LOG.warning("Unknown stdout message {}", msg)
  }
}

class StdErr(input: InputStream, executor: ActorRef) extends Actor {
  import scala.language.postfixOps
  val LOG = Logging(context.system, this)
  override def preStart {
    try { 
      val hamaHome = System.getProperty("hama.home.dir")
      val dir = new File("%s/logs".format(hamaHome))
      if(!dir.exists) dir.mkdirs
      val out = 
        new FileOutputStream(new File(dir, "%s.err".format(executor.path.name)))
      Iterator.continually(input.read).takeWhile(-1!=).foreach(out.write) 
    } catch { 
      case e: Exception => LOG.error("Fail reading stderr {}.", e) 
    } finally { 
      input.close 
      executor ! StreamClosed 
    }
  }

  override def receive = { 
    case msg@_ => LOG.warning("Unknown stderr message {}", msg)
  }
}

/**
 * An actor forks a child process for executing tasks.
 * @param conf cntains necessary setting for launching the child process.
 */
class Executor(conf: HamaConfiguration) extends Actor {
 
  val LOG = Logging(context.system, this)
  val pathSeparator = System.getProperty("path.separator")
  val fileSeparator = System.getProperty("file.separator")
  val javaHome = System.getProperty("java.home")
  val hamaHome = System.getProperty("hama.home.dir")
  val javacp: String  = "./:"+System.getProperty("java.class.path")
  val logPath: String = System.getProperty("hama.log.dir")
  val taskManagerName = conf.get("bsp.groom.taskmanager.name", "taskManager") 
  val operation = Operation.create(conf)
  protected var taskManagerListener: ActorRef = _
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
    val command = Seq(java) ++ Seq(opts) ++ 
                  Seq("-classpath") ++ Seq(classpath(hamaHome, cp)) ++
                  Seq(bspClassName) ++ 
                  Seq(taskPort) ++ Seq(slotSeq.toString)
    LOG.info("jvm args: {}", command.mkString(" "))
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
    var cp = "./:%s/conf:%s".format(hamaHome, parentClasspath)
    val lib = new File(hamaHome, "lib")
    lib.listFiles(new FilenameFilter {
      def accept(dir: File, name: String): Boolean = {
        var flag = false
        if(lib.equals(dir) && name.endsWith("jar") && 
           !name.contains("commons-logging")) {
          flag = true
        } 
        flag
      }
    }).foreach( jar => { cp += ":"+jar })
    LOG.debug("Classpath: {}", cp)
    cp
  }

  /**
   * Fork a child process based on command assembled.
   * @param slotSeq indicate which seq the slot is.
   * @param conf contains specific setting for creating child process.
   */
  def fork(slotSeq: Int, conf: HamaConfiguration) {
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
    if(null == workDir) {
      val fsDir = operation.getWorkingDirectory
      LOG.debug("Use file system's working directory {}", fsDir.toString)
      conf.set("bsp.working.dir", fsDir.toString)
      workDir = fsDir.toString
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
                                     self),
                               "stdout%s".format(slotSeq)) 
      stderr = context.actorOf(Props(classOf[StdErr], 
                                     process.getErrorStream, 
                                     self),
                               "stderr%s".format(slotSeq)) 
    } catch {
      case ioe: IOException => 
        LOG.error("Fail launching BSPPeerContainer process {}", ioe)
    }
  }

  /**
   * Taks manager register itself for notification.
   * @return Receive is partial function.
   */
  def register: Receive = {
    case Register => {
      sender.path.name match {
        case `taskManagerName` => {
          taskManagerListener = sender
          LOG.info("TaskManager {} registers.", taskManagerListener.path.name)
        }
        case rest@_ => LOG.warning("Only accept task manager but {} found.", 
                                   rest)
      }
    }
  }

  /**
   * Create a container for executing tasks that will assign to it.
   * @return Receive partial function.
   */
  def fork: Receive = {
    case Fork(slotSeq, conf) => {
      if(0 >= slotSeq) 
        throw new IllegalArgumentException("Invalid slotSeq: "+slotSeq)
      this.slotSeq = slotSeq
      fork(slotSeq, conf) 
    }
  } 

  /**
   * Once the stream, including input and error stream, is closed, the system
   * will destroy process automatically.
   */
  def streamClosed: Receive = {
    case StreamClosed => {
      LOG.info("{} notifies InputStream is closed!", sender.path.name)
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
   * BSPPeerContainer notify it's in ready state.
   * @return Receive is partial function.
   */
  def containerReady: Receive = {
    case ContainerReady => {
      bspPeerContainer = sender
      if(null != taskManagerListener) {
        LOG.info("Notify {} ContainerReady by {}", 
                 taskManagerListener.path.name, self.path.name)
        taskManagerListener ! PullForExecution(slotSeq) 
      }
    }
  }

  /**
   * Notify when BSPPeerContainer is stopped.
   * @return Receive is partial function.
   */
  def containerStopped: Receive = {
    case ContainerStopped => {
      if(null != taskManagerListener) taskManagerListener ! ContainerStopped
    }
  }

  /**
   * Send StopContainer message to shutdown BSPPeerContainer process.
   * @return Receive is partial function.
   */
  def stopProcess: Receive = {
    case StopProcess => {
      if(null != bspPeerContainer) 
        bspPeerContainer ! StopContainer 
      else 
        LOG.warning("Can't stop BSPPeerContainer for slot {} is not yet "+
                    "ready.", slotSeq)
    }
  }

  def shutdownSystem: Receive = {
    case ShutdownSystem => {
      if(null != bspPeerContainer) 
        bspPeerContainer ! ShutdownSystem 
      else 
        LOG.warning("Can't shutdown because BSPPeerContainer for slot {} is "+ 
                    "not ready.", slotSeq)
    }
  }

  def unknown: Receive = {
    case msg@_=> LOG.warning("Unknown message {} for Executor", msg)
  }

  def terminated: Receive = {
    case Terminated(target) => LOG.info("{} is offline.", target.path.name)
  }

  def receive = register orElse containerReady orElse fork orElse streamClosed orElse stopProcess orElse containerStopped orElse terminated orElse shutdownSystem orElse unknown
     
}

