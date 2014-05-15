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

/*
import akka.actor._
import akka.event._
import java.io.BufferedReader
import java.io.BufferedWriter
import java.io.File
import java.io.FileWriter
import java.io.InputStream
import java.io.InputStreamReader
import java.io.IOException
import java.lang.management._
import java.lang.ProcessBuilder
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import org.apache.commons.lang.math.NumberUtils
import org.apache.hadoop.io.IOUtils
import org.apache.hama.groom.BSPPeerContainer
import org.apache.hama.util.RunJar
import org.apache.hama.util.BSPNetUtils
import org.apache.hama.HamaConfiguration
import scala.collection.JavaConversions._
import scala.sys.process._

 TODO: send jobFilePath and reconstruct necessary information, including job id, jar path, task attempt id, etc., in child process.
final case class Fork(jobId: String, jobFile: String, jarPath: String, 
                      taskAttemptId: String, slotSeq: Int, 
                      conf: HamaConfiguration)

//final case class Fork(slotSeq: Int, conf: HamaConfiguration)

sealed trait LogMessage
final case class StdOutMsg(input: InputStream) extends LogMessage
final case class StdErrMsg(error: InputStream) extends LogMessage

sealed trait LogType
final case object Stdout extends LogType
final case object Stderr extends LogType
final case object Console extends LogType

trait LogAssistant {

  def logStream(input: InputStream, logType: LogType, logDir: File, 
                taskAttemptId: String, conf: HamaConfiguration) {
    logType match { 
      case Console => try {
        IOUtils.copyBytes(input, System.out, conf)
      } catch {
        case ioe: IOException => throw ioe
      }
      // STDOUT file can be found under LOG_DIR/task_attempt_id.log
      // ERROR file can be found under LOG_DIR/task_attempt_id.err
      case t@ _ => {
        val end = t match { 
          case Stdout => "log"
          case Stderr => "err"
          case Console =>
        }
        val taskLogFile = new File(logDir, taskAttemptId+"."+end)
        var writer: BufferedWriter  = null
        try {
          writer = new BufferedWriter(new FileWriter(taskLogFile))
          val in = new BufferedReader(new InputStreamReader(input))
          var line: String = null
          while(null != (line = in.readLine)) {
            writer.write(line)
            writer.newLine
          }
        } catch {
          case ioe: IOException => throw ioe
        }
      }
    }
  }
}

class StdOut(logDir: File, taskAttemptId: String, conf: HamaConfiguration) 
    extends Actor with LogAssistant {

  val LOG = Logging(context.system, this)

  def receive = {
    case StdOutMsg(input) => 
      logStream(input, Stdout, logDir, taskAttemptId, conf) 
    case msg@_ => LOG.warning("Unknown StdOut message {}", msg)
  } 
}

class StdErr(logDir: File, taskAttemptId: String, conf: HamaConfiguration) 
    extends Actor with LogAssistant {
  val LOG = Logging(context.system, this)
  def receive = {
    case StdErrMsg(error) => 
      logStream(error, Stderr, logDir, taskAttemptId, conf) 
    case msg@_ => LOG.warning("Unknown StdErr message {}", msg)
  } 
}

 * An actor forks a new child process in executing tasks.
 * @param conf cntains necessary setting for launching the child process.
class Executor(conf: HamaConfiguration) extends Actor {
 
  val LOG = Logging(context.system, this)

  //type PID = Int

  val pathSeperator = System.getProperty("path.separator")
  val javaHome = System.getProperty("java.home")

  def logPath: String = System.getProperty("hama.log.dir")
  def javacp: String = System.getProperty("java.class.path")

  def workingDir(jobFilePath: String): File = {
    val workDir = new File(new File(jobFilePath).getParent(), "work")
    val isCreated = workDir.mkdirs
    if (isCreated) 
      LOG.info("Working directory is created at {}", workDir)
    workDir
  }

   * Log directory, in a form of "log/tasklogs/job_id"
   * @param logPath is the System.getProperty("hama.log.dir")
   * @param jobId 
  def loggingDir(logPath: String, jobId: String): File = {
    val logDir = new File(logPath + File.separator + "tasklogs" + 
                          File.separator + jobId)
    if (!logDir.exists) logDir.mkdirs
    logDir
  }

  def unjar(jarPath: String, workDir: File) {
    try {
      RunJar.unJar(new File(jarPath), workDir)
    } catch {
      case ioe: IOException => 
        LOG.error("Fail unjar to directory {}", workDir.toString)
    }
  }

  def libsPath(workDir: File): String = {
    val builder = new StringBuilder()
    val libs = new File(workDir, "lib").listFiles
    if (null != libs) {
      libs.foreach(lib => {
        builder.append(pathSeperator)
        builder.append(lib)
      })
    }
    builder.toString
  }

   * @param javaClasspath denotes the classpath from 
   *                      System.getProperty("java.class.path")
   * @param jarPath is the file containing bsp job.
   * @param workDir denotes the working directory.
  def classpath(javaClasspath: String, jarPath: String, workDir: File): 
      String = {
    val classPath = new StringBuilder()
    if(null != javaClasspath && !javaClasspath.isEmpty) {
      classPath.append(javaClasspath)
      classPath.append(pathSeperator)
    }
    if(null != jarPath && !jarPath.isEmpty) {
      unjar(jarPath, workDir)
      classPath.append(libsPath(workDir))
      classPath.append(pathSeperator)
      classPath.append(new File(workDir, "classes"))
      classPath.append(pathSeperator)
      classPath.append(workDir) 
    }
    classPath.toString
  }

   * Address to be used to launch the child process, default to 127.0.0.1.
  def taskAddress: String = conf.get("bsp.child.address", "127.0.0.1")

   * Pick up a port value configured in HamaConfiguration object. Otherwise
   * use default 50001.
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

  private def jvmArgs(javaHome: String, classpath: String, child: Class[_], 
                      slotSeq: Int): 
      Seq[String] = {
    val java = new File(new File(javaHome, "bin"), "java").toString
    LOG.debug("Arg java: {}", java)
    val opts = defaultOpts.split(" ")
    LOG.debug("Arg opts: {}", opts)
    val bspClassName = child.getName 
    LOG.debug("Arg bspClasName: {}", bspClassName)
    val command = Seq(java) ++ opts.toSeq ++ Seq(bspClassName) ++ 
                  Seq(taskPort) ++ Seq(slotSeq.toString)
    LOG.debug("jvm args: {}", command)
    command
  }

   * @param jarPath is the path obtained from conf.get("bsp.jar").
  def fork(jobId: String, jobFilePath: String, jarPath: String, 
           taskAttemptId: String, slotSeq: Int, 
           conf: HamaConfiguration) {
    val workDir = workingDir(jobFilePath)
    val logDir = loggingDir(logPath, jobId)
    LOG.info("jobId {} logDir is {}", jobId, logDir)
    val cp = classpath(javacp, jarPath, workDir)
    LOG.info("jobId {} classpath: {}", jobId, cp)
    val cmd = jvmArgs(javaHome, cp, classOf[BSPPeerContainer], slotSeq)
    LOG.info("jobId {} cmd: {}", jobId, cmd)
    createProcess(cmd, workDir, logDir, taskAttemptId, conf)
  }

  def fork(slotSeq: Int, conf: HamaConfiguration) {

  }
  

   * @param logDir is the directory for a running task with particular forked  
   *               process.
  def createProcess(cmd: Seq[String], workDir: File, logDir: File, 
                    taskAttemptId: String, conf: HamaConfiguration) {
    if(!logDir.exists) logDir.mkdirs
    val builder = new ProcessBuilder(asJavaList(cmd))
    builder.directory(workDir)
    try {
      val process = builder.start

      val out = context.actorOf(Props(classOf[StdOut], logDir, 
                                                       taskAttemptId, 
                                                       conf)) 
      out ! StdOutMsg(process.getInputStream)

      val err = context.actorOf(Props(classOf[StdErr], logDir, 
                                                       taskAttemptId, 
                                                       conf)) 
      err ! StdErrMsg(process.getErrorStream)

    } catch {
      case ioe: IOException => 
        LOG.error("Fail launching BSPPeerContainer process {}", ioe)
    }
  }

  def forkProcess: Receive = {
    // TODO: need refactor/ reduce parameters.
    case Fork(jobId, jobFilePath, jarPath, taskAttemptId, slotSeq, 
              conf) => 
      fork(jobId, jobFilePath, jarPath, taskAttemptId, slotSeq, conf) 
  } 

  def unknown: Receive = {
    case msg@_=> LOG.warning("Unknown message {} for Executor", msg)
  }

  def receive = forkProcess orElse unknown
     
}
*/
