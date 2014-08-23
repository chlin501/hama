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
   * This is intended to be used by {@link BSPPeerContainer} only.
   * @param taskLogParam contains {@link ActorSystem}, log dir path, and 
   *                     slot seq, indicating to which slot this task logger
   *                     belongs.
   */
  def apply[L <: TaskLogger](taskLogParam: TaskLogParam, 
                             taskLoggerClass: Class[L]): LoggingAdapter = {
    val sys = taskLogParam.system
    checkIfNull("ActorSystem", sys) 
    val logDir = taskLogParam.logDir
    checkIfEmpty(logDir)
    val logger = sys.actorOf(Props(taskLoggerClass, logDir), 
                          "taskLogger%s".format(taskLogParam.slotSeq))
    checkIfNull("TaskLogger", logger)
    new TaskLogging(logger)
  }
 
  private def checkIfNull(msg: String, arg: Any) = arg match {
    case null => throw new IllegalArgumentException(msg+" not provided!")
    case _ =>
  }

  private def checkIfEmpty(arg: String) = arg match {
    case null|"" => 
      throw new IllegalArgumentException("Task logDir not provided!")
    case _ =>
  }
}

/**
 * This is intended to be used by implementation with different purposes.
 */
trait LoggingAdapter { 

  def info(msg: String, args: Any*)

  def debug(msg: String, args: Any*)

  def warning(msg: String, args: Any*)

  def error(msg: String, args: Any*)

  def format(msg: String, args: Any*): String = 
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

  final protected[logging] case class Initialize(taskAttemptId: TaskAttemptID)
  final protected[logging] case class Info(message: String)
  final protected[logging] case class Debug(message: String)
  final protected[logging] case class Warning(message: String)
  final protected[logging] case class Error(message: String)
  final protected[logging] case class Close(taskAttemptId: TaskAttemptID)

}

/** 
 * Wrapper for task logger.
 */
protected[logging] class TaskLogging(logger: ActorRef) extends LoggingAdapter {

  import TaskLogging._

  def initialize(taskAttemptId: TaskAttemptID) = 
    logger ! Initialize(taskAttemptId)

  override def info(msg: String, args: Any*) = 
    logger ! Info(format(msg, args:_*))

  override def debug(msg: String, args: Any*) = 
    logger ! Debug(format(msg, args:_*))

  override def warning(msg: String, args: Any*) = 
    logger ! Warning(format(msg, args:_*))

  override def error(msg: String, args: Any*) = 
    logger ! Error(format(msg, args:_*))

  def close(taskAttemptId: TaskAttemptID) = logger ! Close(taskAttemptId)

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

/*
trait ExecutorLogger {

  def write conf, ext, f: () => Unit...

} TODO

StdOut extends Actor with ExecutorLogger
StdErr extends Actor with ExecutorLogger
*/

/**
 * This is intended to be used by {@link BSPPeerContainer} for task logging.
 * @param logDir points to the log path directory, under which job id dirs 
 *               with differrent task attempt ids would be created for logging.
 */
protected[logging] class TaskLogger(logDir: String) extends Actor {

  import TaskLogging._

  type StdOutWriter = FileWriter
  type StdErrWriter = FileWriter
  type JobIDPath = String

  protected var out: Option[StdOutWriter] = None
  protected var err: Option[StdErrWriter] = None
  protected var taskAttemptId: Option[TaskAttemptID] = None

  override def receive = {
    case Initialize(taskAttemptId) => {
      this.taskAttemptId = taskAttemptId match {
        case null => None
        case _ => Some(taskAttemptId)  
      }
      val (stdout, stderr) = mkPathAndWriters(logDir, taskAttemptId, mkdirs)
      out = stdout
      err = stderr
    }
    case Info(msg) => write(out, msg) 
    case Debug(msg) => write(out, msg)
    case Warning(msg) => write(err, msg)
    case Error(msg) => write(err, msg)
    case Close(attemptId) => {
      attemptId match {
        case null => println("Don't know close which task attempt id for it's"+
                             " missing!")
        case id@_ => this.taskAttemptId match {
          case Some(targetId) => closeIfMatched(id.toString, targetId.toString)
          case None => System.err.println("TaskAttemptId not matched => id: "+
                                          id+", task attempt id: None.")
        }
      }
    }
    case msg@_ => println("Unknown msg "+msg+" found for task attempt id "+
                          taskAttemptId.getOrElse("'unknown'"))
  }

  protected def closeIfMatched(id: String, 
                               currentId: String) = id.equals(currentId) match {
    case true => {
      closeIfNotNull(out)
      closeIfNotNull(err)
    }
    case false => println("Id "+id+" doesn't match current task attempt id "+
                          currentId)
  }

  protected def closeIfNotNull(fw: Option[FileWriter]) = fw match {
    case Some(found) => try {} finally { found.close }
    case None =>
  }

  protected def write(writer: Option[FileWriter], msg: String) = writer match {
    case Some(found) => found.write(msg+"\n")
    case None => 
      System.err.println("Unlikely! But either stdout or stderr is missing!")
  }

  protected def mkdirs(logDir: String, jobId: String): (JobIDPath, Boolean) = {
    val jobDir = new File(logDir, jobId)
    val ret = jobDir.exists match {
      case false => jobDir.mkdirs
      case true => true 
    }
    (jobDir.toString, ret)
  }

  protected def mkPathAndWriters(logDir: String,
                                 taskAttemptId: TaskAttemptID, 
                                 mkdirs: (String, String) => 
                                         (JobIDPath, Boolean)): 
      (Option[StdOutWriter], Option[StdErrWriter]) = {
    val jobId = taskAttemptId.getJobID.toString
    mkdirs(logDir, jobId) match {
      case (jobDir, true) => 
        (Some(getWriter(jobDir, taskAttemptId.toString)),
         Some(getWriter(jobDir, taskAttemptId.toString, ".err")))
      case (jobDir, false) => (None, None)
    }
  }

  protected def getWriter(jobDir: String, 
                          taskAttemptId: String, 
                          ext: String = "log",
                          append: Boolean = true): FileWriter = 
    new FileWriter(new File(jobDir, taskAttemptId+"."+ext), append)

}

final case class TaskLogParam(system: ActorSystem, 
                              logDir: String, 
                              slotSeq: Int)

/**
 * Used by TaskLog and underlying Task actor.
 */
protected[logging] trait TaskLogParameter {

  def getTaskLogParam(): TaskLogParam 
}

/**
 * Intended to be used by {@link BSPPeerContainer}.
 */
trait TaskLog extends HamaLog { self: TaskLogParameter => // TODO:change to Actor? so we can use context.actorOf to create log actor ref

  override def LOG: LoggingAdapter = {
    // val logger = context.actorOf(Props(classOf[TaskLogger], logDir), 
    //                              "taskLogger%s".format(slotSeq))
    // Logging(logger)
    Logging[TaskLogger](getTaskLogParam, classOf[TaskLogger])
  }

}
