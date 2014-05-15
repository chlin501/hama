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
import akka.event.Logging
import java.io.BufferedReader
import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter
import java.io.InputStream
import java.io.InputStreamReader
import java.io.IOException
import java.io.OutputStream
import java.lang.ProcessBuilder
import org.apache.commons.lang.math.NumberUtils
import org.apache.hadoop.io.IOUtils
import org.apache.hama.groom.BSPPeerContainer
import org.apache.hama.HamaConfiguration
import org.apache.hama.fs.Operation
import org.apache.hama.util.BSPNetUtils
import scala.collection.JavaConversions._

final case class Fork(slotSeq: Int, conf: HamaConfiguration)
final case class StdOutMsg(logDir: String, logFile: String) 
final case class StdErrMsg(logDir: String, logFile: String)
final case class LogWith(jobId: String, taskAttemptId: String)
final case class StreamClosed

trait LogToFile {

  def error(msg: String, e: IOException) 
  def warn(msg: String, e: IOException)
  def info(msg: String)

  def redirect(conf: HamaConfiguration): Boolean = 
    conf.getBoolean("hama.child.redirect.log.console", false)

  def logToConsole(input: InputStream, conf: HamaConfiguration) {
    try {
      IOUtils.copyBytes(input, System.out, conf)
    } catch {
      case ioe: IOException => error("Fail logging to console", ioe)
    }
  }

  def logStream(input: InputStream, logPath: File, process: ActorRef) {
    var writer: BufferedWriter = null;
    try {
      writer = new BufferedWriter(new FileWriter(logPath))
      var in: BufferedReader = new BufferedReader(new InputStreamReader(input))
      var line: String = null
      while (null != (line = in.readLine)) {
        writer.write(line)
        writer.newLine
      }
    } catch {
      case ioe: IOException => 
        error("Fail logging to %s".format(logPath.toString), ioe)
    } finally {
      try { input.close } catch { case ioe: IOException => }
      try { writer.close } catch { case ioe: IOException => }
      info("InputStream is closed!")
      process ! StreamClosed 
    }
  }
}

class StdOut(input: InputStream, conf: HamaConfiguration, process: ActorRef) 
      extends Actor with LogToFile {

  val LOG = Logging(context.system, this)

  def error(msg: String, ioe: IOException) = LOG.error(msg, ioe)
  def warn(msg: String, ioe: IOException) = LOG.warning(msg, ioe)
  def info(msg: String) = LOG.info(msg)

  def receive = {
    case StdOutMsg(logDir, logFile) => { 
      if(redirect(conf)) logToConsole(input, conf) else {
        if(null != logDir && !logDir.isEmpty) {
          val logPath = new File(logDir, logFile+".log")
          logStream(input, logPath, process) 
        } else LOG.warning("Invalid log dirrectory for stdout!")
      }
    }
    case msg@_ => LOG.warning("Unknown stdout message {}", msg)
  } 
}

class StdErr(input: InputStream, conf: HamaConfiguration, process: ActorRef) 
      extends Actor with LogToFile {

  val LOG = Logging(context.system, this)
  def error(msg: String, ioe: IOException) = LOG.error(msg, ioe)
  def warn(msg: String, ioe: IOException) = LOG.warning(msg, ioe)
  def info(msg: String) = LOG.info(msg)

  def receive = {
    case StdErrMsg(logDir, logFile) => {
      if(redirect(conf)) logToConsole(input, conf) else {
        if(null != logDir && !logDir.isEmpty) {
          val logPath = new File(logDir, logFile+".err")
          logStream(input, logPath, process) 
        } else LOG.warning("Invalid log dirrectory for stdout!")
      }
    }
    case msg@_ => LOG.warning("Unknown stderr message {}", msg)
  } 
}

/**
 * An actor forks a child process for executing tasks.
 * @param conf cntains necessary setting for launching the child process.
 */
class Process(conf: HamaConfiguration) extends Actor {
 
  val LOG = Logging(context.system, this)
  val pathSeparator = System.getProperty("path.separator")
  val fileSeparator = System.getProperty("file.separator")
  val javaHome = System.getProperty("java.home")
  val javacp: String  = System.getProperty("java.class.path")
  val logPath: String = System.getProperty("hama.log.dir")
  val operation = Operation.create(conf)
  var stdout: ActorRef = _
  var stderr: ActorRef = _

  /**
   * Pick up a port value configured in HamaConfiguration object. Otherwise
   * use default 50001.
   */
  def taskPort: String = {
    var port = "50001" 
    val ports = conf.getStrings("bsp.child.port", "50001")
    if(1 != ports.length) {
      var cont = true
      ports.takeWhile( p => {
        if(NumberUtils.isDigits(p)) {
          if(BSPNetUtils.available(p.toInt)) {
            port = p
            cont = false
          }
        } 
        cont
      })
    } else {
      port = ports(0)
    }
    port
  }

  // Bug: it seems using conf.get the stack lost track
  //      following execution e.g. LOG.info after conf.get disappers 
  def defaultOpts: String = conf.get("bsp.child.java.opts", "-Xmx200m")

  def jvmArgs(cp: String, slotSeq: Int, child: Class[_]): Seq[String] = {
    val java = new File(new File(javaHome, "bin"), "java").toString
    val opts = defaultOpts.split(" ")
    val bspClassName = child.getName
    val command = Seq(java) ++ opts.toSeq ++ Seq("-classpath") ++ 
                  Seq(cp) ++ Seq(bspClassName) ++ Seq(taskPort) ++ 
                  Seq(slotSeq.toString)
    LOG.info("jvm args: {}", command)
    command
  }

  def fork(slotSeq: Int, conf: HamaConfiguration) {
    val cmd = jvmArgs(javacp, slotSeq, classOf[BSPPeerContainer])
    createProcess(cmd, conf) 
  }
  
  def defaultWorkingDirectory(conf: HamaConfiguration): String = {
    var workDir = conf.get("bsp.working.dir")
    if(null == workDir) {
      val fsDir = operation.getWorkingDirectory
      LOG.info("Use file system's working directory {}", fsDir.toString)
      conf.set("bsp.working.dir", fsDir.toString)
      workDir = fsDir.toString
    }
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
      val process = builder.start
      stdout = context.actorOf(Props(classOf[StdOut], 
                                     process.getInputStream, 
                                     conf, self)) 
      stderr = context.actorOf(Props(classOf[StdErr], 
                                     process.getErrorStream, 
                                     conf, self)) 
      val exitCode = process.waitFor
      if(0 != exitCode)
        throw new IOException("Child process exist with code = "+exitCode+
                              ", command = "+cmd.mkString(" ")+"!")
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
    case Fork(slotSeq, conf) => fork(slotSeq, conf) 
  } 

  def logDir(jobId: String): String = {
    val log = new File(logPath + fileSeparator + "tasklogs" + fileSeparator + 
                       jobId);
    if (!log.exists) log.mkdirs
    log.toString
  }

  def stdoutAndStderrExists: Boolean = (null != stdout && null != stderr)

  def switchLog: Receive = {
    case LogWith(jobId, taskAttemptId) => { 
      if(null != jobId && !jobId.isEmpty) {
        if(stdoutAndStderrExists) {
          stdout ! StdOutMsg(logDir(jobId), taskAttemptId)
          stderr ! StdErrMsg(logDir(jobId), taskAttemptId)
        } else LOG.warning("Standard out or standard err is missing!")
      } else LOG.warning("Unknown logging path!")
    }
  }

  def streamClosed: Receive = {
    case StreamClosed => {
      LOG.info("{} notify InputStream is closed!", sender.path.name)
    }
  }

  def unknown: Receive = {
    case msg@_=> LOG.warning("Unknown message {} for Executor", msg)
  }

  def receive = fork orElse switchLog orElse streamClosed orElse unknown
     
}

