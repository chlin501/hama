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

import akka.actor._
import org.apache.hama._
import org.apache.hama.master.Register
import org.apache.hama.bsp.v2.GroomServerSpec

class Registrator(conf: HamaConfiguration) extends LocalService {

  val groomManagerInfo =
    ProxyInfo("groomManager",
              conf.get("bsp.master.actor-system.name", "MasterSystem"),
              conf.get("bsp.master.address", "127.0.0.1"),
              conf.getInt("bsp.master.port", 40000),
              "bspmaster/groomManager")

  val groomManagerPath = groomManagerInfo.path

  private val host = conf.get("bsp.groom.hostname", "0.0.0.0")
  private val port = conf.getInt("bsp.groom.rpc.port", 50000)

  val groomServerName = "groom_"+host+"_"+port
  val groomHostName = host

  override def configuration: HamaConfiguration = conf

  override def name: String = "registrator"

/*
  def register {
    proxies.get("groomManager") match {
      case Some(groomManager) => {
        groomManager ! Register(new GroomServerSpec(groomServerName, 
                                                    groomHostName, 
                                                    port,
                                                    3))
      }
      case None => throw new RuntimeException("Proxy groomManager not found!")
    }
  }
*/

  override def initializeServices {
    //lookup("groomManager", groomManagerPath)
  }

/*
  def isGroomManagerReady: Receive = {
    case ActorIdentity(`groomManagerPath`, Some(groomManager)) => {
      link("groomManager", groomManager)
      register
    }
    case ActorIdentity(`groomManagerPath`, None) => {
      LOG.info("{} is not yet available.", groomManagerPath)
    }
  }

  def timeout: Receive = {
    case Timeout(proxy) => {
      if("groomManager".equals(proxy))
        lookup("groomManager", groomManagerPath)
      else
        LOG.warning("Unknown proxy {} lookup!", proxy)
    }
  }
*/

  override def receive = {
    isServiceReady /*orElse isGroomManagerReady orElse timeout*/ orElse unknown
  }
}
