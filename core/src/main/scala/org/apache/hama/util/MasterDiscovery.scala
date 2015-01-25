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

import org.apache.hama.HamaConfiguration
import org.apache.hama.ProxyInfo
import org.apache.hama.RemoteService
import org.apache.hama.conf.Setting
import org.apache.zookeeper.CreateMode

object MasterLookupException {

  def apply(message: String): MasterLookupException = 
    new MasterLookupException(message)

  def apply(message: String, cause: Throwable): MasterLookupException = {
    val e = new MasterLookupException(message)
    e.initCause(cause)
    e
  }
}

class MasterLookupException(message: String) extends RuntimeException(message) 

object MasterDiscovery {

  val pattern = """(\w+)_(\w+)@(\w+):(\d+)""".r

}

trait MasterDiscovery extends RemoteService with Curator { 

  import MasterDiscovery._

  protected var master: Option[ProxyInfo] = None

  protected def setting(): Setting

  protected def needRegister(): Boolean = 
    setting.hama.getBoolean("master.need.register", true) 
 
  protected def mkPath(): String = {
    val sys = setting.info.getActorSystemName
    val host = setting.info.getHost
    val port = setting.info.getPort
    "/%s/%s_%s@%s:%s".format("masters", setting.name, sys, host, port)
  }

  protected def register() = if(needRegister) startCurator(setting.hama) match {
    case true => {
      val path = mkPath
      LOG.debug("Master znode will be registered at {}", path)
      create(path, CreateMode.EPHEMERAL)
    }
    case false => throw MasterLookupException("Can't start curator!")
  }  

  protected def discover(): ProxyInfo = masters match {
    case m: Array[ProxyInfo] if 1 != m.size =>
      throw MasterLookupException("Invalid master size: " + m.size + "!")
    case m: Array[ProxyInfo] if 1 == m.size => m(0) 
  }

  protected def masters(): Array[ProxyInfo] = startCurator(setting.hama) match {
    case true => list("/masters").map { child => {
      LOG.debug("Master znode found: {}", child)
      val conf = new HamaConfiguration
      pattern.findAllMatchIn(child).map { m =>
        val name = m.group(1)
        conf.set("master.name", name)
        val sys = m.group(2)
        conf.set("master.actor-system.name", sys)
        val host = m.group(3)
        conf.set("master.host", host)
        val port = m.group(4).toInt
        conf.setInt("master.port", port)
        new ProxyInfo.MasterBuilder(name, conf).build
      }.toArray match {
        case ary: Array[ProxyInfo] if 0 == ary.size =>
          throw new MasterLookupException("Can't formulate master from "+child)
        case ary: Array[ProxyInfo] => ary(0)
      }
    }}.toArray
    case false => Array.empty[ProxyInfo]
  }

  override protected def retryCompleted(name: String, ret: Any) = name match {
    case "discover" => {
      val m = ret.asInstanceOf[ProxyInfo]
      master = Option(m)
      lookup(m.getActorName, m.getPath)
    }
    case _ =>{ LOG.error("Unexpected result {} after lookup!", ret); shutdown }
  }

  override protected def retryFailed(name: String, cause: Throwable) = {
    LOG.error("Shutdown system due to error {} when trying {}", cause, name)
    shutdown
  }

  
}




