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

import akka.actor._
import akka.event._
import com.typesafe.config._
import org.apache.hama.master._
import scala.concurrent.duration._

/**
 * TODO: stores in HamaConfiguration, translating to Config
 */
object MasterConfig {

  def toConfig(): Config = 
    ConfigFactory.parseString("""
      master {
        akka {
          remote.netty.tcp.port = 40000
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
          remote {
            netty.tcp {
              hostname = "127.0.0.1" # read from HamaConfiguration
            }
          }
        }
      }
    """)
}

object MasterRunner {

  def main(args: Array[String]) {
    val conf = new HamaConfiguration() 
    val system = 
      ActorSystem(conf.get("bsp.master.actor-system.name", "MasterSystem"),
                  MasterConfig.toConfig.getConfig("master"))
    system.actorOf(Props(classOf[MasterRunner], conf), "masterRunner")
  }
}

class MasterRunner(conf: HamaConfiguration) extends Actor {
  
  val LOG = Logging(context.system, this)

  var master: ActorRef = _

  override def preStart {
    master = context.system.actorOf(Props(classOf[Master], conf), "bspmaster")
    LOG.info("Subscribe to receive notificaiton when master is in ready state.")
    master ! SubscribeState(Normal, self)
  }

  def receive = {
    case Ready(systemName) =>  LOG.info("{} is in Normal state!", systemName) 
    case Halt(systemName) => {
      LOG.info("{} services are stopped. Shutdown the system...", systemName)
      context.system.shutdown       
    }
  }
}
