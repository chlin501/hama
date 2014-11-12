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
import scala.util.Try
import scala.util.Success
import scala.util.Failure

object Setting {

  def master(): Setting = master(new HamaConfiguration)

  def master(conf: HamaConfiguration): Setting = new MasterSetting(conf)

  def groom(): Setting = groom(new HamaConfiguration)

  def groom(conf: HamaConfiguration): Setting = new GroomSetting(conf)

}

trait Setting {

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

  def info(system: String, host: String, port: Int): SystemInfo

  def info(): SystemInfo

  def name(): String 

  def main(): Class[Actor] 

  def sys(): String 

  def host(): String

  def port(): Int 

  protected def toClass[A <: Actor](name: String): Try[Class[A]] = 
    Try(Class.forName(name).asInstanceOf[Class[A]])

}

class MasterSetting(conf: HamaConfiguration) extends Setting {


  override def hama(): HamaConfiguration = conf

  override def config(): Config = ConfigFactory.parseString(" master { " + 
    akka(host, port, "master") + " }").getConfig("master")

  override def info(system: String, host: String, port: Int): SystemInfo = 
    new SystemInfo(system, host, port)

  override def info(): SystemInfo = {
    info(sys, host, port)
  }

  override def name(): String = conf.get("master.name", "bspmaster")

  override def main(): Class[Actor] = {
    val name = conf.get("master.main", classOf[BSPMaster].getName)
    toClass[BSPMaster](name) match {
      case Success(clazz) => clazz.asInstanceOf[Class[Actor]]
      case Failure(cause) => classOf[BSPMaster].asInstanceOf[Class[Actor]] 
    }
  }

  override def sys(): String = 
    conf.get("master.actor-system.name", "MasterSystem")

  override def host(): String = conf.get("master.host", 
                                         InetAddress.getLocalHost.getHostName)

  override def port(): Int = conf.getInt("master.port", 40000)

}

class GroomSetting(conf: HamaConfiguration) extends Setting {

  override def hama(): HamaConfiguration = conf

  override def config(): Config = ConfigFactory.parseString(" groom { " + 
    akka(host, port, "groom") + " }").getConfig("groom")

  override def info(system: String, host: String, port: Int): SystemInfo = 
    new SystemInfo(system, host, port)

  override def info(): SystemInfo = info(sys, host, port)

  override def name(): String = conf.get("groom.name", "groom")

  override def main(): Class[Actor] = {
    val name = conf.get("groom.main", classOf[GroomServer].getName)
    toClass[GroomServer](name) match {
      case Success(clazz) => clazz.asInstanceOf[Class[Actor]] 
      case Failure(cause) => classOf[GroomServer].asInstanceOf[Class[Actor]] 
    }
  }

  override def sys(): String = 
    conf.get("groom.actor-system.name", "GroomSystem")

  override def host(): String = conf.get("groom.host", 
                                         InetAddress.getLocalHost.getHostName)

  override def port(): Int = conf.getInt("groom.port", 50000)


}
