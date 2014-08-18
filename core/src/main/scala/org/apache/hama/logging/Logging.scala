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
import akka.actor.ActorSystem

import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory

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
 * @param log is an instance of {@link akka.event.Logging}.
 */
class ActorLogging(log: akka.event.LoggingAdapter) extends LoggingAdapter {

  override def info(msg: String, args: Any*) = log.info(format(msg, args))

  override def debug(msg: String, args: Any*) = log.debug(format(msg, args))

  override def warning(msg: String, args: Any*) = log.warning(format(msg, args))

  override def error(msg: String, args: Any*) = log.error(format(msg, args))
 
}

/**
 * A class that wraps common logging.
 * @param log is an instance of {@link org.apache.commons.logging.Log}.
 */
class CommonLogging(log: Log) extends LoggingAdapter {

  override def info(msg: String, args: Any*) = log.info(format(msg, args))

  override def debug(msg: String, args: Any*) = log.debug(format(msg, args))

  override def warning(msg: String, args: Any*) = log.warn(format(msg, args))

  override def error(msg: String, args: Any*) = log.error(format(msg, args))
   
}

trait HamaLog {

  def log(): LoggingAdapter

}

/**
 * Intended to be used by {@link Actor}.
 */
trait ActorLog extends HamaLog { self: Actor =>

  override def log(): LoggingAdapter = Logging(context.system, this)

}

/**
 * Intended to be used by general object.
 */
trait CommonLog extends HamaLog {
  
  override def log(): LoggingAdapter = Logging(getClass)

}
