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
package org.apache.hama.conf

import akka.actor.Actor
import java.net.InetAddress
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.apache.hama.HamaConfiguration
import org.apache.hama.SystemInfo
import org.apache.hama.master.BSPMaster
import org.apache.hama.groom.GroomServer
import org.apache.hama.groom.Container
import scala.util.Try
import scala.util.Success
import scala.util.Failure

object Setting {

  // TODO: rename to fromString?
  def toConfig(content: String): Config = ConfigFactory.parseString(content)

  def change(systemName: String, nodes: Setting*) = nodes.foreach ( node => 
    node.isInstanceOf[MasterSetting] match {
      case true => node.hama.set("master.actor-system.name", systemName) 
      case false => node.isInstanceOf[GroomSetting] match {
        case true => node.hama.set("groom.actor-system.name", systemName)
        case false => throw new RuntimeException("Unsupported setting "+node)
      }
    }
  )

  def master(): Setting = master(new HamaConfiguration)

  def master(conf: HamaConfiguration): Setting = new MasterSetting(conf)

  def groom(): Setting = groom(new HamaConfiguration)

  def groom(conf: HamaConfiguration): Setting = new GroomSetting(conf)

  def container(): Setting = container(new HamaConfiguration)

  def container(conf: HamaConfiguration): Setting = new ContainerSetting(conf)

}

trait Setting {

  import Setting._

  protected def akka(host: String, port: Int, role: String): String = s"""
    akka {
      actor {
        provider = "akka.cluster.ClusterActorRefProvider"
        serializers {
          java = "akka.serialization.JavaSerializer"
          proto = "akka.remote.serialization.ProtobufSerializer"
          writable = "org.apache.hama.io.serialization.WritableSerializer"
        }
        serialization-bindings {
          "com.google.protobuf.Message" = proto
          "org.apache.hadoop.io.Writable" = writable
        }
      }
      remote.netty.tcp {
        hostname = "$host"
        port = $port
      }
      cluster { 
        roles = [$role]
        auto-down-unreachable-after = 10s
      }
    }
  """

  def hama(): HamaConfiguration 

  def config(): Config

  def info(): SystemInfo

  /**
   * This is generally the name of start up point for the actor system. 
   */
  def name(): String 

  def main(): Class[Actor] 

  /**
   * All nodes in the same cluster should use the same actor system name.
   */
  def sys(): String 

  def host(): String

  def port(): Int 

  protected def toClass[A <: Actor](name: String): Try[Class[A]] = 
    Try(Class.forName(name).asInstanceOf[Class[A]])

}

class MasterSetting(conf: HamaConfiguration) extends Setting {

  import Setting._

  override def hama(): HamaConfiguration = conf

  override def config(): Config = toConfig(" master { " + 
    akka(host, port, "master") + " }").getConfig("master")

  protected def info(system: String, host: String, port: Int): SystemInfo = 
    new SystemInfo(system, host, port)

  override def info(): SystemInfo = info(sys, host, port)

  override def name(): String = conf.get("master.name", 
                                         classOf[BSPMaster].getSimpleName)

  override def main(): Class[Actor] = {
    val name = conf.get("master.main", classOf[BSPMaster].getName)
    toClass[BSPMaster](name) match {
      case Success(clazz) => clazz.asInstanceOf[Class[Actor]]
      case Failure(cause) => classOf[BSPMaster].asInstanceOf[Class[Actor]] 
    }
  }

  override def sys(): String = conf.get("master.actor-system.name", "BSPSystem")

  override def host(): String = conf.get("master.host", 
                                         InetAddress.getLocalHost.getHostName)

  override def port(): Int = conf.getInt("master.port", 40000)

}

class GroomSetting(conf: HamaConfiguration) extends Setting {

  import Setting._

  override def hama(): HamaConfiguration = conf

  override def config(): Config = toConfig(" groom { " + 
    akka(host, port, "groom") + " }").getConfig("groom")

  protected def info(system: String, host: String, port: Int): SystemInfo = 
    new SystemInfo(system, host, port)

  override def info(): SystemInfo = info(sys, host, port)

  override def name(): String = conf.get("groom.name", 
    classOf[GroomServer].getSimpleName)

  override def main(): Class[Actor] = {
    val name = conf.get("groom.main", classOf[GroomServer].getName)
    toClass[GroomServer](name) match {
      case Success(clazz) => clazz.asInstanceOf[Class[Actor]] 
      case Failure(cause) => classOf[GroomServer].asInstanceOf[Class[Actor]] 
    }
  }

  override def sys(): String = conf.get("groom.actor-system.name", "BSPSystem")

  override def host(): String = conf.get("groom.host", 
                                         InetAddress.getLocalHost.getHostName)

  override def port(): Int = conf.getInt("groom.port", 50000)

}

class ContainerSetting(conf: HamaConfiguration) extends Setting {

  import Setting._

  protected def content(listeningTo: String, port: Int): String = s"""
    akka {
      actor {
        provider = "akka.remote.RemoteActorRefProvider"
        serializers {
          java = "akka.serialization.JavaSerializer"
          proto = "akka.remote.serialization.ProtobufSerializer"
          writable = "org.apache.hama.io.serialization.WritableSerializer"
        }
        serialization-bindings {
          "com.google.protobuf.Message" = proto
          "org.apache.hadoop.io.Writable" = writable
        }
      }
      remote.netty.tcp {
        hostname = "$listeningTo"
        port = $port
      }
    }
  """

  override def hama(): HamaConfiguration = conf

  override def config(): Config = toConfig(" container { " + 
    content(host, port) + " }").getConfig("container")

  protected def info(system: String, host: String, port: Int): SystemInfo = 
    new SystemInfo(system, host, port)

  override def info(): SystemInfo = info(sys, host, port)

  override def name(): String = conf.get("container.name", 
    classOf[Container].getSimpleName)

  override def main(): Class[Actor] = {
    val name = conf.get("container.main", classOf[Container].getName)
    toClass[Container](name) match {
      case Success(clazz) => clazz.asInstanceOf[Class[Actor]] 
      case Failure(cause) => classOf[Container].asInstanceOf[Class[Actor]] 
    }
  }

  override def sys(): String = 
    conf.get("bsp.actor-system.name", "BSPSystem")

  override def host(): String = conf.get("bsp.peer.hostname", 
                                         InetAddress.getLocalHost.getHostName)

  override def port(): Int = conf.getInt("bsp.peer.port", 61000)

  
}
