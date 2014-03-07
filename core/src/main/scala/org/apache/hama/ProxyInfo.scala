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

case class ProxyInfo(actorName: String,
                     system: String, 
                     host: String,
                     port: Int) {
  val path: String = "akka.tcp://"+system+"@"+host+":"+port+"/user/"+actorName
}

/*
final class MasterProxyBuilder(conf: HamaConfiguration) 
      extends ProxyBuilder(conf) {
  override val system = 
    Some(conf.get("bsp.master.actor-system.name", "bspmaster"))
  override val host = Some(conf.get("bsp.master.address", "localhost"))
  override val port = Some(conf.get("bsp.master.port", 40000))
}

final class ProxyBuilder(conf: HamaConfiguration) {
  private var actorName: Option[String] = None
  private var system: Optoion[String] = None
  private var host: Optoion[String] = None
  private var port: Optoion[Int] = None
  def withActorName(name: String): ProxyBuilder = {
    if(null != name) actorName = Some(name) else actorName = None
    this
  }
  def withSystem(sys: String): ProxyBuilder = {
    if(null != sys) system = Some(sys) else system = None
    this
  }
  def withHost(h: String): ProxyBuilder = {
    if(null != h) host = Some(h) else host = None
    this
  }
  def withPort(p: Int): ProxyBuilder = {
    if(0 < p) port = Some(p) else port = None
    this
  }
  def build: ProxyInfo = ProxyInfo(actorName, system, host, port)
}

case class ProxyInfo(actorName: Option[String],
                     system: Option[String], 
                     host: Option[String],
                     port: Option[Int]) {
  def path: Either[IllegalArgumentException, String] = { 
    val hasActorName = actorName getOrElse(None)
    val hasSystem = system.getOrElse(None)
    val hasHost = host.getOrElse(None)
    val hasPort = port.getOrElse(None) 
    if(None == hasActorName || None == hasSystem || None == hasHost || 
       None == hasPort) {
      Left(new IllegalArgumentException("Invalid proxy information!"+
                                        "actor name: "+hasActorName+
                                        "system: "+hasSystem+
                                        "host: "+hasHost+" port: "+hasPort))  
    } else {
      Right("akka.tcp://"+system+"@"+host+":"+port+"/user/"+actorName)
    }
  }
}
*/

