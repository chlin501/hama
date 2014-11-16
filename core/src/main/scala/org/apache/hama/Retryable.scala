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
package org.apache.hama

import akka.actor.Actor
import akka.actor.Cancellable
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration
import scala.util.Try
import scala.util.Success
import scala.util.Failure

sealed trait RetryMessages
final case class Retry[R](name: String, count: Int, f:() => R) 
      extends RetryMessages

trait Retryable { this: Actor => 

  protected var retryCancellables = Map.empty[String, Cancellable]

  protected def retry(name: String, count: Int, f: () => Any, 
                         delay: FiniteDuration = 3.seconds) {
    import context.dispatcher
    val cancellable = context.system.scheduler.scheduleOnce(delay, 
      self, Retry(name, count, f))
    retryCancellables ++= Map(name -> cancellable)
  }

  protected def retryCompleted(name: String, result: Any) { }

  protected def retryFailed(name: String, cause: Throwable) { }

  protected def retryResult: Receive = {
    case Retry(name, count, f) => Try(f()) match {
      case Success(ret) => {
        cancelRetry(name) 
        retryCompleted(name, ret)
      }
      case _ if count > 1 => retry(name, (count-1), f)
      case Failure(cause) => {
        cancelRetry(name) 
        retryFailed(name, cause)
      }
    }
  }

  protected def cancelRetry(name: String) = retryCancellables.get(name) match {
    case Some(found) => { 
      found.cancel
      retryCancellables -= name 
    }
    case None => 
  }

}
