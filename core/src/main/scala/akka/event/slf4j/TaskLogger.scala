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
package akka.event.slf4j

import org.slf4j.MDC
import akka.event.Logging._
import akka.actor._

class TaskLogger extends Slf4jLogger {

  val logPath = "log_path"

  override def receive = {

    case event @ Error(cause, logSource, logClass, message) ⇒
      withMdcx(logSource, event, message) {
        cause match {
          case Error.NoCause | null ⇒ Logger(logClass, logSource).error(if (rip(message) != null) rip(message).toString else null)
          case _                    ⇒ Logger(logClass, logSource).error(if (rip(message) != null) rip(message).toString else cause.getLocalizedMessage, cause)
        }
      }

    case event @ Warning(logSource, logClass, message) ⇒
      withMdcx(logSource, event, message) { Logger(logClass, logSource).warn("{}", rip(message).asInstanceOf[AnyRef]) }

    case event @ Info(logSource, logClass, message) ⇒
      withMdcx(logSource, event, message) { Logger(logClass, logSource).info("{}", rip(message).asInstanceOf[AnyRef]) }

    case event @ Debug(logSource, logClass, message) ⇒
      withMdcx(logSource, event, message) { Logger(logClass, logSource).debug("{}", rip(message).asInstanceOf[AnyRef]) }

    case InitializeLogger(_) ⇒
      sender ! LoggerInitialized
  }

  final def rip(msg: Any): String = {
    msg.toString.split("-") match {
      case ary: Array[String] if 2 == ary.size => ary(1).trim
      case x@_ => { println("Can't retrieve from original message: "+x); ""}
    }
  }

  @inline
  final def withMdcx(logSource: String, logEvent: LogEvent, message: Any)(logStatement: ⇒ Unit) {
    val path = message.toString.split("-") match {
      case ary: Array[String] => ary(0).trim
      case x@_ => ""
    }
    MDC.put(logPath, path)
    MDC.put(mdcAkkaSourceAttributeName, logSource)
    MDC.put(mdcThreadAttributeName, logEvent.thread.getName)
    MDC.put(mdcAkkaTimestamp, formatTimestamp(logEvent.timestamp))
    try logStatement finally {
      MDC.remove(logPath)
      MDC.remove(mdcAkkaSourceAttributeName)
      MDC.remove(mdcThreadAttributeName)
      MDC.remove(mdcAkkaTimestamp)
    }
  }
}
