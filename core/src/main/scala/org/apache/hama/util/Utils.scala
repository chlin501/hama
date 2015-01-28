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
package org.apache.hama.util

import akka.actor.ActorRef
import akka.actor.ActorPath
import akka.actor.Address
import akka.actor.ChildActorPath
import akka.actor.RootActorPath
import akka.pattern.ask
import akka.util.Timeout
import org.apache.hadoop.io.NullWritable
import org.apache.hama.ProxyInfo
import org.apache.hama.SystemInfo
import org.apache.hama.logging.CommonLog
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.util.Try
import scala.util.Success
import scala.util.Failure

object Utils extends CommonLog {


  def await[R <: Any: ClassTag](caller: ActorRef, message: Any, 
                                defaultTimeout: FiniteDuration = 10.seconds,
                                duration: Duration = Duration.Inf): R = {
    import scala.language.postfixOps
    implicit val timeout = Timeout(defaultTimeout) //10 seconds 
    val future = ask(caller, message).mapTo[R] 
    Await.result(future, duration)
  }

  def actorPath(info: ProxyInfo): ActorPath = {
    val protocol = info.getProtocol.toString
    val sys = info.getActorSystemName
    val host = info.getHost
    val port = info.getPort
    val actorPath = info.getActorPath
    var fullPath: ActorPath = 
      new ChildActorPath(RootActorPath(Address(protocol, sys, host, port)), 
                         "user")
    actorPath.split("/").foreach( node => 
      fullPath = new ChildActorPath(fullPath, node)
    )
    fullPath
  }

  /**
   * Convert from address to system info.
   */ 
  def from(addr: Address): SystemInfo = {
    val host = addr.host.getOrElse(null)
    if(null == host) throw new RuntimeException("Remote host is not provided!")
    val port = addr.port.getOrElse(-2)
    if(-2 == port) throw new RuntimeException("Remote port is not provided!")
    new SystemInfo(addr.protocol, addr.system, host, port)
  }

  def nullString(): String = NullWritable.get.toString

  def isNullString(compared: String): Boolean = nullString.equals(compared)

  def time[R](f: => R, name: String = ""): R = {
    val start = System.nanoTime
    val result = f
    val end = System.nanoTime
    val elapsed = end - start
    LOG.debug("Execute function {} took {} secs: start from {} ended at {}.", 
              name, (elapsed/1000000000d), start, end)
    result
  }

}

