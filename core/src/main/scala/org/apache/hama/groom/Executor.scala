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
import akka.actor.ActorRef
import akka.actor.Terminated
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
import org.apache.hama.Agent
import org.apache.hama.HamaConfiguration
import org.apache.hama.Spawnable
import org.apache.hama.fs.Operation
import org.apache.hama.monitor.Report
import org.apache.hama.util.BSPNetUtils
import scala.collection.immutable.Queue
import scala.collection.JavaConversions._

trait ExecutorMessages
/**
 * Slot sequence value is smaller than 0 (inclusive).
 */
final case class IllegalSlotSequence(seq: Int) extends ExecutorMessages
final case class Command(msg: Any, recipient: ActorRef) extends ExecutorMessages
final case object StreamClosed extends ExecutorMessages   

trait ExecutorLog { // TODO: refactor this after all log is switched 

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

// TODO: refactor using logging.Logging
class StdOut(input: InputStream, conf: HamaConfiguration, executor: ActorRef) 
      extends Agent with ExecutorLog { 
  
  override def preStart = 
    log(name, input, conf, executor, "log", LOG.error)

  override def receive = { 
    case msg@_ => LOG.warning("Unknown stdout message {}", msg)
  }
}

// TODO: refactor using logging.Logging
class StdErr(input: InputStream, conf: HamaConfiguration, executor: ActorRef) 
      extends Agent with ExecutorLog {

  override def preStart = 
    log(name, input, conf, executor, "err", LOG.error)

  override def receive = { 
    case msg@_ => LOG.warning("Unknown stderr message {}", msg)
  }
}

object Executor {

  val javaHome = System.getProperty("java.home")
  val hamaHome = System.getProperty("hama.home.dir")
  val javacp: String  = System.getProperty("java.class.path")
  val logPath: String = System.getProperty("hama.log.dir")
}

/**
 * An actor forks a child process for executing tasks.
 * @param conf cntains necessary setting for launching the child process.
 */
class Executor(conf: HamaConfiguration, taskCounsellor: ActorRef) 
      extends Agent with Spawnable {

  import Executor._

  type CommandQueue = Queue[Command]

  val operation = Operation.get(conf)
  var commandQueue = Queue[Command]()
  protected var container: Option[ActorRef] = None 
  protected var stdout: Option[ActorRef] = None
  protected var stderr: Option[ActorRef] = None
  protected var isStdoutClosed = false
  protected var isStderrClosed = false

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

  // Note: it seems when using conf.get the stack lost track, e.g. LOG.info, 
  //       after conf.get  
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
    val bspPeerSystemName = conf.get("bsp.child.actor-system.name", 
                                        "BSPPeerSystem")
    // decide the host to which remote module will listen, default to 0.0.0.0 
    val listeningTo = conf.get("bsp.peer.hostname", "0.0.0.0") 
    val command = Seq(java) ++ Seq(opts) ++  
                  Seq("-classpath") ++ Seq(classpath(hamaHome, cp)) ++
                  Seq(bspClassName) ++ Seq(bspPeerSystemName) ++ 
                  Seq(listeningTo) ++ Seq(taskPort) ++ Seq(slotSeq.toString)
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
    val containerClass = conf.getClass("bsp.child.class", classOf[Container])
    LOG.debug("Container class to be instantiated is {}", containerClass)
    val cmd = javaArgs(javacp, slotSeq, containerClass)
    createProcess(slotSeq, cmd, conf) 
  }

  /**
   * Fork a child process as container.
   * @param cmd is the command to excute the process.
   * @param conf contains related information for creating process.
   */
  def createProcess(seq: Int, cmd: Seq[String], conf: HamaConfiguration) {
    val builder = new ProcessBuilder(seqAsJavaList(cmd))
    builder.directory(new File(Operation.defaultWorkingDirectory(conf)))
    try { // TODO: use Try[Boolean] instead
      val process = builder.start
      stdout = Option(spawn("stdout%s".format(seq), classOf[StdOut], 
                            process.getInputStream, conf, self))
                               
      stderr = Option(spawn("stderr%s".format(seq), classOf[StdErr], 
                            process.getErrorStream, conf, self))
    } catch {
      case ioe: IOException => LOG.error("Fail launching Container process {}",
                                         ioe) // TODO: notify task counsellor 
    }
  }

  /**
   * Create a container for executing tasks that will assign to it.
   * @return Receive partial function.
   */
  def fork: Receive = {
    case Fork(seq) => seq match {
      case x if x <= 0 => sender ! IllegalSlotSequence(seq)
      case _ => fork(seq) 
    }
  } 

  /**
   * Ask {@link Container} to launch a task.
   * This should happens after {@link Container} is ready.
   * @param Receive is partial function.
   */
  def launchTask: Receive = {
    case action: LaunchTask => container.map { c => 
      c ! new LaunchTask(action.task)
    }
  }

  def launchAck: Receive = {
    case action: LaunchAck => 
      taskCounsellor ! new LaunchAck(action.slotSeq, action.taskAttemptId) 
  }

  /** 
   * Ask {@link Container} to resume a specific task.
   * This should happens after {@link Container} is ready.
   * @param Receive is partial function.
   */
  def resumeTask: Receive = {
    case action: ResumeTask => container.map { c => 
      c ! new ResumeTask(action.task)
    }
  }


  def resumeAck: Receive = {
    case action: ResumeAck => 
      taskCounsellor ! new ResumeAck(action.slotSeq, action.taskAttemptId)
  }

  /**
   * Ask {@link Container} to kill the task that is currently running.
   * This should happens after {@link Container} is ready.
   * @param Receive is partial function.
   */
  def killTask: Receive = {
    case action: KillTask => container.map { c => 
      c ! new KillTask(action.taskAttemptId)
    }
  }

  protected def killAck: Receive = {
    case action: KillAck => 
      taskCounsellor ! new KillAck(action.slotSeq, action.taskAttemptId)
  }

  protected def stdoutName(): String = stdout.map { s => s.path.name }.
    getOrElse(null)

  protected def stderrName(): String = stderr.map { s => s.path.name }.
    getOrElse(null)

  /**
   * Once the stream, including input and error stream, is closed, the system
   * will destroy process automatically.
   */
  protected def streamClosed: Receive = {
    case StreamClosed => {
      LOG.debug("{} notifies InputStream is closed!", sender.path.name)
      if(sender.path.name.equals(stdoutName)) { 
        isStdoutClosed = true
      } else if(sender.path.name.equals(stderrName)) {
        isStderrClosed = true
      } else LOG.warning("[Warning] Sender {} asks for closing stream.", 
                         sender.path.name)
      if(isStdoutClosed && isStderrClosed) self ! StopProcess
    }
  }

  /**
   * Container notify when it's in ready state.
   * @return Receive is partial function.
   */
  def containerReady: Receive = {
    case ContainerReady(seq) => {
      container = Option(sender)
      while(!commandQueue.isEmpty) {
        val (cmd, rest) = commandQueue.dequeue
        container.map { c => c ! cmd.msg }
        commandQueue = rest  
      }
      afterContainerReady(seq, taskCounsellor)
    }
  }

  def afterContainerReady(seq: Int, target: ActorRef) = 
    target ! PullForExecution(seq) 

  /**
   * Notify when Container is stopped.
   * @return Receive is partial function.
   */
  def containerStopped: Receive = {
    case ContainerStopped => taskCounsellor ! ContainerStopped
  }

  /**
   * Send StopContainer message to shutdown Container process.
   * @return Receive is partial function.
   */
  def stopProcess: Receive = {
    case StopProcess => container match {
      case None => 
        commandQueue = commandQueue.enqueue(Command(StopContainer, sender)) 
      case Some(c) => c ! StopContainer 
    }
  }

  /**
   * Issue shutdown command to {@link Container}, which shuts down the
   * child process.
   * @param Receive is partial function.
   */
  def shutdownContainer: Receive = {
    case ShutdownContainer => {
      container match {
        case None => commandQueue = 
          commandQueue.enqueue(Command(ShutdownContainer, sender)) 
        case Some(c) => {
          LOG.debug("Shutdown container {}", c)
          c ! ShutdownContainer 
        }
      }
    }
  }

  protected def terminated: Receive = {
    case Terminated(target) => LOG.warning("{} is offline.", target.path.name)
  }

  /**
   * Reply to {@link TaskCounsellor} that slot is occupied!
   * @return Receive is partial function.
   */
  protected def occupied: Receive = {
    case result: Occupied => {
      LOG.warning("Slot {} is occupied by {}.", result.getSlotSeq, 
                  result.getTaskAttemptId.toString)
      taskCounsellor ! result
    }
  }

  protected def report: Receive = {
    case r: Report => taskCounsellor ! r
  }

  def receive = launchAck orElse occupied orElse resumeAck orElse killAck orElse launchTask orElse resumeTask orElse killTask orElse containerReady orElse fork orElse streamClosed orElse stopProcess orElse containerStopped orElse terminated orElse shutdownContainer orElse report orElse unknown
     
}

