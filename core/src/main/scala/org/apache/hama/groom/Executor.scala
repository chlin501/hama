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
import akka.actor.SupervisorStrategy.Stop
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
import org.apache.hama.Service
import org.apache.hama.Spawnable
import org.apache.hama.conf.Setting
import org.apache.hama.fs.Operation
import org.apache.hama.logging.CommonLog
import org.apache.hama.util.BSPNetUtils
import scala.collection.immutable.Queue
import scala.collection.JavaConversions._
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration.DurationInt
import scala.util.Failure
import scala.util.Success
import scala.util.Try

sealed trait ExecutorMessage
final case class Instances(process: Process, stdout: ActorRef, 
                           stderr: ActorRef) extends ExecutorMessage
final case class Destroy(seq: Int) extends ExecutorMessage

sealed trait ExecutorException extends RuntimeException {

  def slotSeq(): Int

}

object ClasspathException {

  def apply(slotSeq: Int, msg: String): ClasspathException = {
    val cause = new RuntimeException(msg)
    apply(slotSeq, cause)
  }

  def apply(slotSeq: Int, cause: Throwable): ClasspathException = {
    val e = new ClasspathException(slotSeq)
    e.initCause(cause)
    e
  }

}

final class ClasspathException(seq: Int) extends ExecutorException {

  def slotSeq(): Int = seq

}

object ProcessFailureException {

  def apply(slotSeq: Int, cause: Throwable): ProcessFailureException = {
    val e = new ProcessFailureException(slotSeq)
    e.initCause(cause)
    e
  }

}

final class ProcessFailureException(seq: Int) extends ExecutorException {

  def slotSeq(): Int = seq

  override def toString(): String = "Fail creating process for slot "+seq+"!"

}


trait ExecutorLog { // TODO: refactor this after all log is switched Logging

  import Executor._

  def log(name: String, input: InputStream, conf: HamaConfiguration, 
          executor: ActorRef, ext: String, error: (String, Any*) => Unit) {
    import scala.language.postfixOps
    try {  
      logPath match {
        case null | "" => logWith(conf, "/tmp/hama/logs", executor, input, ext)
        case _ => logWith(conf, logPath, executor, input, ext) 
      }
    } catch { 
      case e: Exception => {
        error("Fail reading "+name, e) 
        throw e
      }
    } finally { 
      input.close 
    }
  }

  def logWith(conf: HamaConfiguration, logPath: String, executor: ActorRef,
              input: InputStream, ext: String) = 
    if(!conf.getBoolean("groom.executor.log.console", false)) {
      val logDir = new File(logPath)
      if(!logDir.exists) logDir.mkdirs
      val out = new FileOutputStream(new File(logDir, 
        "%s.%s".format(executor.path.name, ext)))
      Iterator.continually(input.read).takeWhile(-1!=).foreach(out.write) 
    } else {
      Iterator.continually(input.read).takeWhile(-1!=).foreach(println) 
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

object Executor extends CommonLog {

  // TODO: group to Setting for unifying access system attributes 
  val javaHome = System.getProperty("java.home")
  val hamaHome = System.getProperty("hama.home.dir")   
  val javacp: String  = System.getProperty("java.class.path") 
  val logPath: String = hamaHome + "/logs"

  def javabin(): String = new File(new File(javaHome, "bin"), "java").
    getCanonicalPath

  def javaOpts(conf: HamaConfiguration): String =  
    conf.get("container.java.opts", "-Xmx200m")

  def simpleName(setting: Setting): String = 
    GroomServer.simpleName(setting) + "_executor_" + 
    setting.getInt("groom.executor.slot.seq", -1)

}

/**
 * An actor forks a child process for executing tasks.
 * @param groomSetting contains values for child process.
 */
class Executor(groomSetting: Setting, slotSeq: Int) extends Service 
                                                       with Spawnable {

  import Executor._

  protected val containerSetting = Setting.container(slotSeq)

  protected var instances: Option[Instances] = None

  override def initializeServices() = fork(containerSetting) 

  /**
   * Pick up a port value configured in HamaConfiguration object. Otherwise
   * use default 50001.
   * @param String of the port value.
   */
  protected def taskPort(): String = BSPNetUtils.getFreePort(61000).toString

  /**
   * Assemble java command for launching the child process.  
   * @param cp is the classpath.
   * @param setting is the container setting.
   */
  protected def javaArgs(cp: String, setting: Setting): Seq[String] = {
    val opts = javaOpts(setting.hama)
    val bspClassName = setting.main.getName 
    val sys = setting.sys
    // decide to which address the peer will listen, default to 0.0.0.0 (?)
    val listeningTo = setting.host
    val port = taskPort
    LOG.debug("Container process will be created at {}{}@{}:{}", sys, slotSeq, 
              listeningTo, port)
    val command = Seq(javabin) ++ Seq(opts) ++  
                  Seq("-classpath") ++ Seq(setting.classpath.mkString(":")) ++ 
                  Seq(bspClassName) ++ Seq(sys) ++ 
                  Seq(listeningTo) ++ Seq(port) ++ Seq(slotSeq.toString)
    LOG.debug("Java command to be executed: {}", command.mkString(" "))
    command
  }

  /**
   * Fork a child process based on command assembled.
   */
  protected def fork(containerSetting: Setting) {
    val cmd = javaArgs(javacp, containerSetting) 
    newContainer(containerSetting, cmd) match {
      case Success(instance) => {
        LOG.info("Container {} exists normally!", slotSeq)
        this.instances = None
      }
      case Failure(cause) => {
        LOG.error("Can't fork child process because {}", cause)
        cleanupInstances
        throw ProcessFailureException(slotSeq, cause)
      }
    }
  }

  /**
   * Create container subprocess.
   * @param setting is the container setting.
   * @param cmd is the command to excute the process.
   */
  protected def newContainer(setting: Setting, cmd: Seq[String]): Try[Int] = 
    try { 
      val conf = setting.hama
      require(null != conf, "Container configuration not found!")
      val seq = setting.hama.getInt("container.slot.seq", -1)
      require(-1 != seq, "Invalid container slot seq!")
      val builder = new ProcessBuilder(seqAsJavaList(cmd))
      builder.directory(new File(Operation.defaultWorkingDirectory(conf)))
      val process = builder.start
      val stdout = spawn("stdout%s".format(seq), classOf[StdOut], 
                         process.getInputStream, conf, self)
      val stderr = spawn("stderr%s".format(seq), classOf[StdErr], 
                         process.getErrorStream, conf, self)
      this.instances = Option(Instances(process, stdout, stderr))
      val exitValue = process.waitFor // blocking call
      require(0 == exitValue, "Non 0 exit value found: "+exitValue)
      Success(exitValue)
    } catch {
      case e: Exception => Failure(e)
    }

  protected def cleanupInstances() = { 
    instances.map { o => {
      o.process.destroy
      context.stop(o.stdout)
      context.stop(o.stderr)
    }} 
    instances = None
  }

  protected def destroy: Receive = {
    case Destroy(seq) => (seq == slotSeq) match {
      case true => cleanupInstances
      case false => LOG.error("Can't stop process because slot seq {} "+
                              "expected, but {} found!", slotSeq, seq)
    } 
  }

  override def receive = destroy orElse unknown
     
}
