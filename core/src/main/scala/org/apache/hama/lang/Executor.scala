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
import org.apache.hama.util.RunJar
import org.apache.hama.HamaConfiguration
import scala.sys.process._

final case class Fork(jobId: String, jobFile: String, jar: String, 
                      taskAttemptId: String)

class Executor(conf: HamaConfiguration) extends Actor {
 
  val LOG = Logging(context.system, this)

  type PID = Int

  val pathSeperator = System.getProperty("path.separator")

  def workingDir(jobFile: String): File = {
    val workDir = new File(new File(jobFile).getParent(), "work")
    val isCreated = workDir.mkdirs
    if (isCreated) 
      LOG.debug("Working directory is created at {}", workDir)
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

  /**
   * @param javaClasspath denotes the classpath from 
   *                      System.getProperty("java.class.path")
   * @param jar is the file containing bsp job.
   * @param workDir denotes the working directory.
   */
  def classpath(javaClasspath: String, jar: String, workDir: File): String = {
    val classPath = new StringBuilder()
    classPath.append(javaClasspath)
    classPath.append(pathSeperator)
    if(null != jar && !"".equals(jar)) {
      try {
        RunJar.unJar(new File(jar), workDir)
      } catch {
        case ioe: IOException => 
          LOG.error("Fail unjar to {}", workDir.toString)
      }
      val libs = new File(workDir, "lib").listFiles
      if (null != libs) {
        libs.foreach(lib => {
          classPath.append(pathSeperator)
          classPath.append(lib)
        })
      }
      classPath.append(pathSeperator)
      classPath.append(new File(workDir, "classes"))
      classPath.append(pathSeperator)
      classPath.append(workDir) 
    }
    classPath.toString
  }

  def taskAddress: String = {
   "127.0.0.1"
  }

  def taskPort: String = {
    "50001"
  }

  // System.getProperty("java.home")
  private def jvmArgs(javaHome: String, taskAttemptId: String, 
                      classpath: String, child: Class[_]): Seq[String] = {
    val java = new File(new File(javaHome, "bin"), "java").toString
    val javaOpts = conf.get("bsp.child.java.opts", "-Xmx200m").
                        replace("@taskid@", taskAttemptId).split(" ")
    //if(!classOf[BSPPeerChild].equals(child)) 
      //throw new RuntimeException("Class {} not match to BSPPeerChild.", child)
    val bspClassName = child.getName 
    //val groomHostName = conf.get("bsp.groom.address", "127.0.0.1")
    //need superstep from which to restart  
    //val superstep = 
    val command = Seq(java) ++ javaOpts.toSeq ++ Seq(bspClassName) ++ 
                  Seq(taskAddress) ++ Seq(taskPort) ++ Seq(taskAttemptId)
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

  def fork(jobId: String, jobFile: String, jar: String, 
           taskAttemptId: String) {
    val workDir = workingDir(jobFile)
    val logPath = System.getProperty("hama.log.dir")
    LOG.debug("logPath pointed to "+logPath)
    val logDir = loggingDir(logPath, jobId)
    val javacp = System.getProperty("java.class.path")
    LOG.debug("java classpath "+javacp)
    val cp = classpath(javacp, jar, workDir)
    val javaHome = System.getProperty("java.home")
    val cmd = jvmArgs(javaHome, taskAttemptId, cp, classOf[BSPChild])
    createProcess(cmd, workDir)
  }

  def createProcess(cmd: Seq[String], workDir: File) {
    val logger = ProcessLogger(line => logStdOut(line),  
                               line => logStdErr(line)) 
    Process(cmd, workDir) ! logger
  }

  // TODO: log to different files. e.g. task_id.log and task_id.err
  /**
   * Write std err to error log 
   */
  def logStdErr(line: String) {
    LOG.error(line)
  }

  /**
   * Write std out to log.
   */
  def logStdOut(line: String) {
    LOG.info(line)
  }

  def forkProcess: Receive = {
    case Fork(jobId, jobFile, jar, taskAttemptId) => {
      fork(jobId, jobFile, jar, taskAttemptId) 
    }
  } 

  def unknown: Receive = {
    case msg@_=> LOG.warning("Unknown message {} for Executor", msg)
  }

  def receive = forkProcess orElse unknown
     
}

object BSPChild {
}

class BSPChild { 
 
  @throws(classOf[Throwable])
  def main(args: Array[String]) {

  }
}