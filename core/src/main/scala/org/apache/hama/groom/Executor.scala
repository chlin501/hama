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
import akka.actor.OneForOneStrategy
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

/**
 * {@link TaskCounsellor} notifies {@link Executor} to stop
 * {@link Container}.
private[groom] final case object StopProcess extends ExecutorMessage
 */


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

}


trait ExecutorLog { // TODO: refactor this after all log is switched Logging

  def log(name: String, input: InputStream, conf: HamaConfiguration, 
          executor: ActorRef, ext: String, error: (String, Any*) => Unit) {
    import scala.language.postfixOps
    try { 
      val logPath = System.getProperty("hama.log.dir")
      logPath match {
        case null | "" => logWith(conf, "/tmp/hama/logs", executor, input, ext)
        case _ => logWith(conf, logPath, executor, input, ext) 
      }
    } catch { 
      case e: Exception => error("Fail reading "+name, e) 
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

object Executor {

  val javaHome = System.getProperty("java.home")
  val hamaHome = System.getProperty("hama.home.dir")
  val javacp: String  = System.getProperty("java.class.path")
  val logPath: String = System.getProperty("hama.log.dir")

  def java(): String = 
    new File(new File(javaHome, "bin"), "java").getCanonicalPath

  def defaultOpts(conf: HamaConfiguration): String = 
    conf.get("container.java.opts", "-Xmx200m")

  def simpleName(conf: HamaConfiguration): String = 
    GroomServer.simpleName(conf) + "_executor_" + 
    conf.getInt("groom.executor.slot.seq", -1)

}

/**
 * An actor forks a child process for executing tasks.
 * @param setting cntains necessary setting for launching the child process.
 */
class Executor(setting: Setting, slotSeq: Int) extends Service with Spawnable {

  import Executor._

  protected var instances: Option[Instances] = None

  override def initializeServices() = fork(slotSeq) 

  /**
   * Pick up a port value configured in HamaConfiguration object. Otherwise
   * use default 50001.
   * @param String of the port value.
   */
  protected def taskPort(): String = {
    val port = BSPNetUtils.getFreePort(50002).toString
    LOG.debug("Port value to be used is {}", port)
    port
  }

  /**
   * Assemble java command for launching the child process.  
   * @param cp is the classpath.
   * @param slotSeq indicates to which slot this process belongs.
   * @param child is the class used to launced the process.
   * @return Seq[String] is the command for launching the process.
   */
  protected def javaArgs(cp: String, slotSeq: Int, child: Class[_]): 
      Seq[String] = {
    val opts = defaultOpts(setting.hama)
    val bspClassName = child.getName
    val actorSysName = setting.sys
    // decide to which address the peer will listen, default to 0.0.0.0 
    val listeningTo = setting.host
    val command = Seq(java) ++ Seq(opts) ++  
                  Seq("-classpath") ++ Seq(classpath(hamaHome, cp)) ++
                  Seq(bspClassName) ++ Seq(actorSysName) ++ 
                  Seq(listeningTo) ++ Seq(taskPort) ++ Seq(slotSeq.toString)
    LOG.debug("Java command to be executed: {}", command.mkString(" "))
    command
  }

  /**
   * Collect jar files found under ${HAMA_HOME}/lib to form the classpath 
   * variable for the child process.
   * @param hamaHome is pointed to hama home.dir directory.
   * @return String of classpath value.
   */
  protected def classpath(hamaHome: String, parentClasspath: String): String = {
    if(null == hamaHome)  // TODO: find better to handle side effect 
      throw ClasspathException(slotSeq, "Variable hama.home.dir is not set!")
    var cp = "./:%s:%s/conf".format(parentClasspath, hamaHome)
    val lib = new File(hamaHome, "lib")
    lib.listFiles(new FilenameFilter {
      def accept(dir: File, name: String): Boolean = true
    }).foreach( jar => { cp += ":"+jar })
    LOG.debug("Classpath is configured to {}", cp)
    cp
  }

  protected def containerClass(): Class[Container] = {
    val clazz = setting.hama.getClass("container.main", classOf[Container])
    LOG.info("Container class to be instantiated: {}", clazz.getName)
    clazz.asInstanceOf[Class[Container]]
  }

  /**
   * Fork a child process based on command assembled.
   * @param slotSeq indicate which seq the slot is.
   */
  protected def fork(slotSeq: Int) {
    val cmd = javaArgs(javacp, slotSeq, containerClass)
    newContainer(slotSeq, cmd, setting.hama) match {
      case Success(instances) => 
        LOG.info("Container process with slot {} exits normally!", slotSeq)
      case Failure(cause) => {
        cleanupInstances
        throw ProcessFailureException(slotSeq, cause)
      }
    }
  }

  /**
   * Create container subprocess.
   * @param cmd is the command to excute the process.
   * @param conf contains related information for creating process.
   */
  protected def newContainer(seq: Int, cmd: Seq[String], 
                             conf: HamaConfiguration): Try[Int] = try { 
    val builder = new ProcessBuilder(seqAsJavaList(cmd))
    builder.directory(new File(Operation.defaultWorkingDirectory(conf)))
    val process = builder.start
    val stdout = spawn("stdout%s".format(seq), classOf[StdOut], 
                       process.getInputStream, conf, self)
                                
    val stderr = spawn("stderr%s".format(seq), classOf[StdErr], 
                       process.getErrorStream, conf, self)
    LOG.info("Container process for slot seq {} is forked ...", seq)
    this.instances = Option(Instances(process, stdout, stderr))
    val exitValue = process.waitFor // blocking call
    LOG.info("Conainer process exits with value {}", exitValue)
    if(0 != exitValue) 
      throw new RuntimeException("Non 0 exit value: "+exitValue)
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

  override def receive = unknown
     
}
