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
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
import akka.testkit.TestActor
import akka.testkit.TestKit
import akka.testkit.TestProbe
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import java.net.InetAddress
import org.apache.hama.conf.Setting
import org.apache.hama.master.BSPMaster
import org.apache.hama.groom.GroomServer
import org.apache.hama.logging.CommonLog
import org.scalatest.BeforeAndAfterAll
import org.scalatest.FunSpecLike
import org.scalatest.ShouldMatchers
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

object MultiNodesEnv {

  def parseString(str: String): Config = ConfigFactory.parseString(str)

  def setting(): Setting = new MultiNodesSetting(new HamaConfiguration)

  def setting(conf: HamaConfiguration): Setting = new MultiNodesSetting(conf)
}

class Tester extends Actor { // TODO: with assert functions for verification

  override def receive = {
    case msg@_ => println("unknown message: "+msg)
  }
}

class MultiNodesSetting(conf: HamaConfiguration) extends Setting {

  def hama(): HamaConfiguration = conf

  override def config(): Config = ConfigFactory.parseString(" test { " + 
    cluster(host, port, "test") + " }").getConfig("test")

  def info(system: String, host: String, port: Int): SystemInfo = 
    new SystemInfo(system, host, port)

  def info(): SystemInfo = info(sys, host, port)

  def name(): String = conf.get("tester.name", "test")

  def main(): Class[Actor] = classOf[Tester].asInstanceOf[Class[Actor]]

  def sys(): String = conf.get("test.actor-system.name", 
                               "TestSystem")

  def host(): String = conf.get("test.host", 
                                InetAddress.getLocalHost.getHostName)

  def port(): Int = conf.getInt("test.port", 10000)
}

class MultiNodesEnv(actorSystem: ActorSystem) 
      extends TestKit(actorSystem) with FunSpecLike 
                                   with ShouldMatchers 
                                   with BeforeAndAfterAll 
                                   with CommonLog {

  import MultiNodesEnv._

  val hostValue = InetAddress.getLocalHost.getHostName

  protected var master: Option[ActorSystem] = None
 
  protected var grooms = Map.empty[String, ActorSystem]

  /**
   * Instantiate test environment with name only.
   * @param name is the actor system name.
   */
  def this(name: String) = this(ActorSystem(name, MultiNodesEnv.setting.config))

  /**
   * Instantiate test environment with name and {@link Config} object.
   * @param name of the actor system.
   * @param config object.
   */
  def this(name: String, conf: HamaConfiguration) = 
    this(ActorSystem(name, MultiNodesEnv.setting(conf).config))

  override protected def afterAll = system.shutdown

  protected def masterSetting[A <: Actor](name: String = "bspmaster", 
                                          actorSystemName: String = "BSPSystem",
                                          main: Class[A] = classOf[BSPMaster], 
                                          host: String = hostValue,
                                          port: Int = 40000): Setting = {
    val master = Setting.master
    LOG.info("Configure master with: name {}, main class {}, port {}", 
             name, main, port)
    master.hama.set("master.name", name)
    master.hama.set("master.actor-system.name", actorSystemName)
    master.hama.set("master.main", main.getName)
    master.hama.set("master.host", host)
    master.hama.setInt("master.port", port)
    master
  }

  protected def groomSetting[A <: Actor](name: String = "groomserver", 
                                         actorSystemName: String = "BSPSystem",
                                         main: Class[A] = classOf[GroomServer], 
                                         host: String = hostValue,
                                         port: Int = 50000): Setting = {
    val groom = Setting.groom
    LOG.info("Configure groom with: name {}, main class {}, port {}", 
             name, main, port)
    groom.hama.set("groom.name", name)
    groom.hama.set("groom.actor-system.name", actorSystemName)
    groom.hama.set("groom.main", main.getName)
    groom.hama.set("groom.host", host)
    groom.hama.setInt("groom.port", port)
    groom
  }

  /**
   * Start an ActorSystem.
   * @param actorSystemName is the name of actor system.
   * @param setting contains config required by the actor system.
   */
  protected def startMaster(setting: Setting): ActorSystem = master match { 
    case Some(found) => found
    case None => {
      val m = ActorSystem(setting.sys, setting.config)
      master = Option(m)
      m
    }  
  }
 
  protected def startGroom(setting: Setting): ActorSystem = {
    val actorSystem = ActorSystem(setting.sys, setting.config)
    grooms ++= Map(setting.name -> actorSystem)
    actorSystem
  }

  protected def startGrooms(settings: Setting*) = settings.foreach( setting =>
    startGroom(setting)
  )

  /**
   * Create either BSPMaster or GroomServer actor.
   */
  protected def masterActorOf[A <: Actor](setting: Setting, 
                                          args: Any*): ActorRef = 
    masterActorOf[Actor](setting.main, setting.name, args: _*)

  /**
   * Create actor with ccoresponded actor system.
   */
  protected def masterActorOf[A <: Actor](main: Class[A], 
                                          name: String, 
                                          args: Any*): ActorRef = master match {
    case Some(found) => found.actorOf(Props(main, args: _*), name)
    case None => throw new RuntimeException("Master is not yet started!")
  }

  protected def groomActorOf[A <: Actor](setting: Setting, 
                                         args: Any*): ActorRef = 
    groomActorOf(setting.main, setting.name, args: _*)

  protected def groomActorOf[A <: Actor](main: Class[A], 
                                         name: String,
                                         args: Any*): ActorRef = 
    grooms.get(name) match {
      case Some(found) => found.actorOf(Props(main, args:_*), name)
      case None => throw new RuntimeException("Can't create startup actor for "+
                                              "no matched name "+ name+"!")
    }
  
  /**
   * Thread sleep {@link FiniteDuration} of time.
   * @param duration with default to 3 seconds.
   */
  protected def sleep(duration: FiniteDuration = 3.seconds) = 
    Thread.sleep(duration.toMillis)

}
