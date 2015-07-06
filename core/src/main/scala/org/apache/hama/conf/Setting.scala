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
import java.io.DataInput
import java.io.DataOutput
import java.io.File
import java.io.FilenameFilter
import java.io.IOException
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.apache.hama.HamaConfiguration
import org.apache.hama.SystemInfo
import org.apache.hama.client.Submitter
import org.apache.hama.groom.GroomServer
import org.apache.hama.groom.Container
import org.apache.hama.master.BSPMaster
import org.apache.hama.util.Utils
import scala.util.Try
import scala.util.Success
import scala.util.Failure

/**
 * Provide akka setting.
 */
protected trait Akka {

  val clusterProvider = "akka.cluster.ClusterActorRefProvider"

  val remoteProvider = "akka.remote.RemoteActorRefProvider"

  protected def akka(content: String): String = " akka { " + content + " } "

  protected def provider(string: String): String = 
    s""" provider = "$string" """

  protected def actor(provider: String): String = 
   " actor { " + provider + serializers + " } "

  protected def serializers(): String = """
    serializers {
      java = "akka.serialization.JavaSerializer"
      proto = "akka.remote.serialization.ProtobufSerializer"
      writable = "org.apache.hama.io.serialization.WritableSerializer"
    }
    serialization-bindings {
      "com.google.protobuf.Message" = proto
      "org.apache.hadoop.io.Writable" = writable
    }
  """

  protected def cluster(host: String, port: Int, role: String): String = 
    akka(actor(provider(clusterProvider)) + s"""
      remote.netty.tcp {
        hostname = "$host"
        port = $port
      }
      cluster { 
        roles = [$role]
        auto-down-unreachable-after = 10s
      }
    """)

  protected def remote(listeningTo: String, port: Int): String = 
    akka(actor(provider(remoteProvider)) + s"""
      remote.netty.tcp {
        hostname = "$listeningTo"
        port = $port
      }
    """)
}

// TODO: merge groom.executor, groom.container system setting to here
trait Hama { 

  /**
   * Obtain Hama's home directory from Java system property "hama.home.dir".
   * @return String that is the representative of this property.
   */
  def hamaHome: String = System.getProperty("hama.home.dir") match {
    case null | "" => System.setProperty("hama.home.dir", 
      System.getProperty("user.dir")) 
    case value@_ => value
  }

  /**
   * The logs directory under hama home.
   * @return String is the representative of the log path. 
   */
  def logs: String = new File(hamaHome, "logs").getCanonicalPath

  /**
   * Java home directory.
   * @return String is the path pointed to JAVA_HOME path.
   */
  def javaHome: String = System.getProperty("java.home") match {
    case null | "" => throw new RuntimeException("JAVA_HOME is not set!")
    case value@_ => new File(new File(value, "bin"), "java").getCanonicalPath
  }

  /**
   * Java binary file.
   * @return String that points to the path of Java binary file within bin 
   *                folder.
   */
  def java: String = new File(new File(javaHome, "bin"), "java").
    getCanonicalPath 

  /**
   * Path used to find directories and JAR archives containing class files.
   * @return String that contains related paths for this process.
   */
  def classpath: Array[String] = System.getProperty("java.class.path") match {
    case null | "" => Array("./")
    case str@_ => str.split(":")
  }

}

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

  /**
   * Create default setting for the master server.
   * @return Setting contians default configuration for the master machine.
   */
  def master(): Setting = master(new HamaConfiguration)

  /**
   * Create setting for the master server with configuration specified.
   * @param conf is the configuration to be applied to the master machine.
   * @return Setting contians configuration sepcific for the master machine.
   */
  def master(conf: HamaConfiguration): Setting = new MasterSetting(conf)

  def groom(): Setting = groom(new HamaConfiguration)

  def groom(conf: HamaConfiguration): Setting = new GroomSetting(conf)

  def container(): Setting = container(new HamaConfiguration)

  def container(slotSeq: Int): Setting = {
    val conf = new HamaConfiguration 
    conf.setInt("container.slot.seq", slotSeq) 
    container(conf)
  }

  def container(sys: String, seq: Int, host: String, port: Int): Setting = {
    val conf = new HamaConfiguration
    require(null != sys && !"".equals(sys) , "Container system name is empty!")
    conf.set("container.actor-system.name", sys)
    require(-1 != seq , "Container slot seq should't be "+seq+"!")
    conf.setInt("container.slot.seq", seq) 
    require(null != host && !"".equals(host) , "Container host is empty!")
    conf.set("container.host", host)
    require(0 < port && 65535 >= port, "Invalud container port value: "+port)
    conf.setInt("container.port", port)
    container(conf)
  }

  def container(conf: HamaConfiguration): Setting = new ContainerSetting(conf)

  def client(): Setting = client(new HamaConfiguration)

  def client(conf: HamaConfiguration): Setting = new ClientSetting(conf)

}

trait Setting extends Akka {

  import Setting._

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

  def get(key: String): String = hama.get(key, null)

  def get(key: String, default: String): String = hama.get(key, default)

  def set(key: String, value: String) = hama.set(key, value)

  def getInt(key: String, default: Int): Int = hama.getInt(key, default)

  def setInt(key: String, value: Int) = hama.setInt(key, value)

  def getLong(key: String, default: Long): Long = hama.getLong(key, default)

  def setLong(key: String, value: Long) = hama.setLong(key, value)

  def getBoolean(key: String, default: Boolean): Boolean = 
    hama.getBoolean(key, default)

  def setBoolean(key: String, value: Boolean) = hama.setBoolean(key, value)

  // TODO: def configuration.getXXXXX e.g. getClass, etc.

}

class MasterSetting(conf: HamaConfiguration) extends Setting {

  import Setting._

  override def hama(): HamaConfiguration = conf

  override def config(): Config = toConfig(" master { " + 
    cluster(host, port, "master") + " }").getConfig("master")

  protected def info(system: String, host: String, port: Int): SystemInfo = 
    new SystemInfo(system, host, port)

  override def info(): SystemInfo = info(sys, host, port)

  override def name(): String = BSPMaster.simpleName(this)

  override def main(): Class[Actor] = {
    val m = conf.get("master.main", classOf[BSPMaster].getName)
    toClass[BSPMaster](m) match {
      case Success(clazz) => clazz.asInstanceOf[Class[Actor]]
      case Failure(cause) => classOf[BSPMaster].asInstanceOf[Class[Actor]] 
    }
  }

  override def sys(): String = conf.get("master.actor-system.name", "BSPSystem")

  override def host(): String = conf.get("master.host", Utils.hostname)

  override def port(): Int = conf.getInt("master.port", 40000)

}

class GroomSetting(conf: HamaConfiguration) extends Setting {

  import Setting._

  override def hama(): HamaConfiguration = conf

  override def config(): Config = toConfig(" groom { " + 
    cluster(host, port, "groom") + " }").getConfig("groom")

  protected def info(system: String, host: String, port: Int): SystemInfo = 
    new SystemInfo(system, host, port)

  override def info(): SystemInfo = info(sys, host, port)

  override def name(): String = GroomServer.simpleName(this)

  override def main(): Class[Actor] = {
    val m = conf.get("groom.main", classOf[GroomServer].getName)
    toClass[GroomServer](m) match {
      case Success(clazz) => clazz.asInstanceOf[Class[Actor]] 
      case Failure(cause) => classOf[GroomServer].asInstanceOf[Class[Actor]] 
    }
  }

  override def sys(): String = conf.get("groom.actor-system.name", "BSPSystem")

  override def host(): String = conf.get("groom.host", Utils.hostname)

  override def port(): Int = conf.getInt("groom.port", 50000)

}

class ContainerSetting(conf: HamaConfiguration) extends Setting with Hama {

  import Setting._

  override def hama(): HamaConfiguration = conf

  override def config(): Config = toConfig(" container { " + 
    remote(host, port) + " }").getConfig("container")

  protected def info(system: String, host: String, port: Int): SystemInfo = 
    new SystemInfo(system, host, port)

  override def info(): SystemInfo = info(sys, host, port)

  override def name(): String = Container.simpleName(this)

  override def main(): Class[Actor] = {
    val m = hama.get("container.main", classOf[Container].getName)
    toClass[Container](m) match {
      case Success(clazz) => clazz.asInstanceOf[Class[Actor]] 
      case Failure(cause) => classOf[Container].asInstanceOf[Class[Actor]] 
    }
  }

  override def sys(): String = hama.get("container.actor-system.name", 
    "BSPSystem")

  // TODO: default listening to 0.0.0.0 ?
  override def host(): String = hama.get("container.host", Utils.hostname)

  override def port(): Int = hama.getInt("container.port", 61000)

  /**
   * Override original classpath setting by including jar files under lib 
   * folder and conf folder.
   * @return Array[String] of all jar files are included under lib directory 
   *                       and conf folder.
   */
  override def classpath: Array[String] = { 
    var cp = "./" +: super.classpath :+ (hamaHome +"/conf")
    val lib = new File(hamaHome, "lib")
    lib.exists match {
      case true => lib.listFiles(new FilenameFilter { 
        def accept(dir: File, name: String): Boolean = true
      }).foreach( jar => cp ++= Array(jar.getCanonicalPath) ) 
      case false => 
    }
    cp.toArray
  }
}

class ClientSetting(conf: HamaConfiguration) extends Setting {

  import Setting._

  override def hama(): HamaConfiguration = conf

  override def config(): Config = toConfig(" client { " + remote(host, port) + 
    " }").getConfig("client")

  protected def info(system: String, host: String, port: Int): SystemInfo = 
    new SystemInfo(system, host, port)

  override def info(): SystemInfo = info(sys, host, port)

  override def name(): String = Submitter.simpleName(this)

  override def main(): Class[Actor] = {
    val m = conf.get("client.main", classOf[Submitter].getName)
    toClass[Submitter](m) match {
      case Success(clazz) => clazz.asInstanceOf[Class[Actor]] 
      case Failure(cause) => classOf[Submitter].asInstanceOf[Class[Actor]] 
    }
  }

  override def sys(): String = conf.get("client.actor-system.name", 
    "BSPSystem")

  override def host(): String = conf.get("client.host", Utils.hostname)

  override def port(): Int = conf.getInt("client.port", 1947)
  
}
