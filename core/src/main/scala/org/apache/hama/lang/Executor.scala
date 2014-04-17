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

import akka.actor._
import akka.event._
import java.io.File
import java.io.IOException
import java.lang.management._
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import org.apache.commons.lang.math.NumberUtils
import org.apache.hama.groom.BSPPeerChild
import org.apache.hama.util.RunJar
import org.apache.hama.util.BSPNetUtils
import org.apache.hama.HamaConfiguration
import scala.sys.process._

final case class Fork(jobId: String, jobFile: String, jarPath: String, 
                      instanceCount: Int)

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

  /**
   * Log directory, in a form of "log/tasklogs/job_id"
   * @param logPath is the System.getProperty("hama.log.dir")
   * @param jobId 
   */
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

  /**
   * @param javaClasspath denotes the classpath from 
   *                      System.getProperty("java.class.path")
   * @param jarPath is the file containing bsp job.
   * @param workDir denotes the working directory.
   */
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

  /**
   * Address to be used to launch the child process, default to 127.0.0.1.
   */
  def taskAddress: String = conf.get("bsp.child.address", "127.0.0.1")

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

  private def jvmArgs(javaHome: String, classpath: String, 
                      child: Class[_], instanceCount: Int): 
      Seq[String] = {
    val java = new File(new File(javaHome, "bin"), "java").toString
    LOG.debug("Arg java: {}", java)
    val opts = defaultOpts.split(" ")
    LOG.debug("Arg opts: {}", opts)
    val bspClassName = child.getName 
    LOG.debug("Arg bspClasName: {}", bspClassName)
    val command = Seq(java) ++ opts.toSeq ++ Seq(bspClassName) ++ 
                  Seq(taskPort) ++ Seq(instanceCount.toString)
    LOG.debug("jvm args: {}", command)
    command
  }

/* In forked actor. send pid back to task manager
  private def pid: PID = {
    val str = ManagementFactory.getRuntimeMXBean.getName
    val ary = str.split("@")
    var ret = -1
    if(null != ary && 0 < ary.size) ret = ary.head.toInt
    if(ret > 65535) 
      throw new IllegalStateException("Pid value can't be larger than 65535.")
    ret 
  }
*/

  /**
   * @param jarPath is the path obtained from conf.get("bsp.jar").
   */
  def fork(jobId: String, jobFilePath: String, jarPath: String, 
           instanceCount: Int) {
    val workDir = workingDir(jobFilePath)
    val logDir = loggingDir(logPath, jobId)
    LOG.info("jobId {} logDir is {}", jobId, logDir)
    val cp = classpath(javacp, jarPath, workDir)
    LOG.info("jobId {} classpath: {}", jobId, cp)
    val cmd = jvmArgs(javaHome, cp, classOf[BSPPeerChild], instanceCount)
    LOG.info("jobId {} cmd: {}", jobId, cmd)
    createProcess(cmd, workDir, logDir)
  }

  /**
   * @param logDir is the directory for a running task with particular forked  
   *               process.
   */
  def createProcess(cmd: Seq[String], workDir: File, logDir: File) {
    if(!logDir.exists) logDir.mkdirs
    val logger = ProcessLogger(line => logStdOut(logDir, line),  
                               line => logStdErr(logDir, line)) 
    Process(cmd, workDir) ! logger
  }

  /**
   * Write std err to error log 
   */
  def logStdErr(logDir: File, line: String) {
    // TODO: write to ${logDir.getPath}/std.err 
    LOG.error(line)
  }

  /**
   * Write std out to log.
   */
  def logStdOut(logDir: File, line: String) {
    // TODO: write ${logDir.getPath}/std.log
    LOG.info(line)
  }

  def forkProcess: Receive = {
    case Fork(jobId, jobFilePath, jarPath, instanceCount) => {
      fork(jobId, jobFilePath, jarPath, instanceCount) 
    }
  } 

  def unknown: Receive = {
    case msg@_=> LOG.warning("Unknown message {} for Executor", msg)
  }

  def receive = forkProcess orElse unknown
     
}
