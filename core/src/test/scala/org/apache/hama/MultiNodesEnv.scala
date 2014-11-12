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
    akka(host, port, "test") + " }").getConfig("test")

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

  protected var systems = Set.empty[ActorSystem]

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

  protected def masterSetting[A <: Actor](name: String, 
                                          main: Class[A], 
                                          port: Int): Setting = {
    val master = Setting.master
    LOG.info("Configure master with: name {}, main class {}, port {}", 
             name, main, port)
    master.hama.set("master.name", name)
    master.hama.set("master.main", main.getName)
    master.hama.setInt("master.port", port)
    master
  }

  protected def groomSetting[A <: Actor](name: String, 
                                         main: Class[A], 
                                         port: Int): Setting = {
    val groom = Setting.groom
    LOG.info("Configure groom with: name {}, main class {}, port {}", 
             name, main, port)
    groom.hama.set("groom.name", name)
    groom.hama.set("groom.main", main.getName)
    groom.hama.setInt("groom.port", port)
    groom
  }

  /**
   * Start an ActorSystem.
   * @param actorSysName is the name of actor system.
   * @param setting contains config required by the actor system.
   */
  protected def start(actorSysName: String, setting: Setting): ActorSystem = 
    systems.find( sys => sys.name.equals(actorSysName) ) match {
      case Some(found) => found
      case None => {
        val actorSystem = ActorSystem(actorSysName, setting.config)
        systems ++= Set(actorSystem)
        actorSystem
      }
    }

  /**
   * Create either BSPMaster or GroomServer actor.
   */
  protected def actorOf[A <: Actor](actorSysName: String, 
                                    setting: Setting, args: Any*): ActorRef = 
    actorOf[Actor](actorSysName, setting.main, setting.name, args:_*)

  /**
   * Create actor with ccoresponded actor system.
   */
  protected def actorOf[A <: Actor](actorSysName: String, main: Class[A], 
                                    name: String, args: Any*): ActorRef = 
    systems.find( sys => sys.name.equals(actorSysName)) match {
      case Some(found) => system.actorOf(Props(main, args:_*), name)
      case None => throw new RuntimeException("No matched ActorSystem name "+
                                              actorSysName+"!")
    }
  
  /**
   * Thread sleep {@link FiniteDuration} of time.
   * @param duration with default to 3 seconds.
   */
  protected def sleep(duration: FiniteDuration = 3.seconds) = 
    Thread.sleep(duration.toMillis)

}
