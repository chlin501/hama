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
package org.apache.hama.logging

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import java.io.File
import java.io.FileWriter
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import org.apache.hama.bsp.TaskAttemptID
import scala.util.Try
import scala.util.Success
import scala.util.Failure

/**
 * This is used to create different log instances.
 */
object Logging {

  /**
   * Create an instance for {@link Actor} logging.
   * @param system is the {@link ActorSystem}.
   * @param a is an instance of Actor.
   * @return LoggingAdapter is a wrapper for underlying logging.
   */
  def apply[A <: Actor](system: ActorSystem, a: A): LoggingAdapter = 
    new ActorLogging(akka.event.Logging(system, a))

  /**
   * Obtain an instance for commons (non-actor) logging.
   * @param clazz is the class to be logged.
   * @return LoggingAdapter wraps a {@link Log} instance for logging.
   */
  def apply(clazz: Class[_]): LoggingAdapter = 
    new CommonLogging(LogFactory.getLog(clazz))

  /**
   * Task log instance will be injected instead of instantiated by worker.
   * @param ref is the task log actor reference.
   * @return LoggingAdapter as the wrapper.
   */
  def apply[L <: TaskLogger](ref: ActorRef) = new TaskLogging(ref)

}

/**
 * A wrapper for different logging mechanism.
 */
trait LoggingAdapter { 

  def info(msg: String, args: Any*)

  def debug(msg: String, args: Any*)

  def warning(msg: String, args: Any*)

  def error(msg: String, args: Any*)

  def format(msg: String, args: Any*): String = Try(f(msg, args:_*)) match {
    case Success(result) => result 
    case Failure(cause) => cause.getMessage match {
      case "Format specifier 's'" => 
        "[Warning] The number of {} and its args specified doesn't match!"
      case errMsg@_ => errMsg
    }
  } 

  private def f(msg: String, args: Any*): String =
    msg.replace("{}", "%s").format(args:_*)

}

/**
 * A class that wraps actor logging.
 * @param logger is an instance of {@link akka.event.Logging}.
 */
protected[logging] class ActorLogging(logger: akka.event.LoggingAdapter) 
      extends LoggingAdapter {

  override def info(msg: String, args: Any*) = 
    logger.info(format(msg, args:_*))

  override def debug(msg: String, args: Any*) = 
    logger.debug(format(msg, args:_*))

  override def warning(msg: String, args: Any*) = 
    logger.warning(format(msg, args:_*))

  override def error(msg: String, args: Any*) = 
    logger.error(format(msg, args:_*))
 
}

/**
 * A class that wraps common logging.
 * @param logger is an instance of {@link org.apache.commons.logging.Log}.
 */
protected[logging] class CommonLogging(logger: Log) extends LoggingAdapter {

  override def info(msg: String, args: Any*) = logger.info(format(msg, args:_*))

  override def debug(msg: String, args: Any*) = 
    logger.debug(format(msg, args:_*))

  override def warning(msg: String, args: Any*) = 
    logger.warn(format(msg, args:_*))

  override def error(msg: String, args: Any*) = 
    logger.error(format(msg, args:_*))
   
}

object TaskLogging {

  final case class Info(message: String)
  final case class Debug(message: String)
  final case class Warning(message: String)
  final case class Error(message: String)

}

/** 
 * Wrapper for task logger.
 */
protected[logging] class TaskLogging(logger: ActorRef) extends LoggingAdapter {
// TODO: filter log level! check ifXXXXXEnabled() { ... } 
  import TaskLogging._

  override def info(msg: String, args: Any*) = 
    logger ! Info(format(msg, args:_*))

  override def debug(msg: String, args: Any*) =  
    logger ! Debug(format(msg, args:_*))

  override def warning(msg: String, args: Any*) = 
    logger ! Warning(format(msg, args:_*))

  override def error(msg: String, args: Any*) = 
    logger ! Error(format(msg, args:_*))

}

/**
 * Client should inherit sub-trait of this one.
 */
trait HamaLog {

  /**
   * Adapter for logging.
   * @return LoggingAdapter that provides logging.
   */
  def LOG: LoggingAdapter

}

/**
 * Intended to be used by {@link Actor}.
 */
trait ActorLog extends HamaLog { self: Actor =>

  override def LOG: LoggingAdapter = Logging(context.system, this)

}

/**
 * Intended to be used by general object.
 */
trait CommonLog extends HamaLog {
  
  override def LOG: LoggingAdapter = Logging(getClass)

}

object TaskLogger {

  val tasklogsPath = "/logs/taskslogs"

}

/**
 * This is intended to be used by {@link Container} for task logging.
 * @param logDir points to the log path directory, under which job id dirs 
 *               with differrent task attempt ids would be created for logging.
 */
protected class TaskLogger(hamaHome: String, taskAttemptId: TaskAttemptID,
                           console: Boolean) extends Actor {

  import TaskLogging._
  import TaskLogger._

  type StdOutWriter = FileWriter
  type StdErrWriter = FileWriter
  type JobIDPath = String

  protected var out: Option[StdOutWriter] = None
  protected var err: Option[StdErrWriter] = None

  def this(hamaHome: String, taskAttemptId: TaskAttemptID) = 
    this(hamaHome, taskAttemptId, false)
  override def preStart() = setup
  override def postStop() = stop

  def logDir(hamaHome: String): String = hamaHome + tasklogsPath

  protected def setup() {
    val (stdout, stderr) = mkPathAndWriters(logDir(hamaHome), taskAttemptId, 
                                            mkdirs)
    out = stdout
    err = stderr
  }

  protected def stop() {
    closeIfNotNull(out)
    closeIfNotNull(err)
  }

  override def receive = {
    case Info(msg) => write(out, msg) 
    case Debug(msg) => write(out, msg)
    case Warning(msg) => write(err, msg)
    case Error(msg) => write(err, msg)
    case msg@_ => println("Unknown msg "+msg+" found for task attempt id "+
                          taskAttemptId)
  }

  protected def closeIfNotNull(fw: Option[FileWriter]) = fw.map { (found) => 
    try {} finally { found.close }
  }

  protected def write(writer: Option[FileWriter], msg: String) = writer match {
    case Some(found) => if(!console) {
      found.write(msg+"\n")
    } else println(msg)
    case None => 
      System.err.println("Either stdout or stderr is missing for task logger!")
  }

  protected def mkdirs(logPath: String, jobId: String): (JobIDPath, Boolean) = {
    val jobDir = new File(logPath, jobId)
    val ret = jobDir.exists match {
      case false => jobDir.mkdirs
      case true => true 
    }
    (jobDir.toString, ret)
  }

  protected def mkPathAndWriters(logsPath: String,
                                 taskAttemptId: TaskAttemptID, 
                                 mkdirs: (String, String) => 
                                         (JobIDPath, Boolean)): 
      (Option[StdOutWriter], Option[StdErrWriter]) = {
    val jobId = taskAttemptId.getJobID.toString
    mkdirs(logsPath, jobId) match {
      case (jobDir, true) => 
        (Some(getWriter(jobDir, taskAttemptId.toString)),
         Some(getWriter(jobDir, taskAttemptId.toString, "err")))
      case (jobDir, false) => (None, None)
    }
  }

  protected def getWriter(jobDir: String, 
                          taskAttemptId: String, 
                          ext: String = "log",
                          append: Boolean = true): FileWriter = 
    new FileWriter(new File(jobDir, taskAttemptId+"."+ext), append)

}


/**
 * Intended to be used by task related actors when different task components
 * are launched.
 */
trait TaskLog extends HamaLog { }
