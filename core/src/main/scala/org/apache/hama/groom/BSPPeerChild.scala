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

import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.ActorRef
import akka.actor.Cancellable
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import org.apache.hama.bsp.TaskAttemptID
import org.apache.hama.bsp.v2.Task
import org.apache.hama.HamaConfiguration
import org.apache.hama.LocalService
import org.apache.hama.ProxyInfo
import org.apache.hama.RemoteService
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.duration.DurationInt

final case class Args(port: Int, instanceCount: Int, config: Config)
final case object Setup

object BSPPeerChild {

  def toConfig(port: Int): Config = {
    ConfigFactory.parseString(s"""
      peer {
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
          remote {
            netty.tcp {
              hostname = "127.0.0.1" 
              port = $port 
            }
          }
        }
      }
    """)
  }

  def toArgs(args: Array[String]): Args = {
    if(null == args || 0 == args.length)
      throw new IllegalArgumentException("No arguments supplied when "+
                                         "BSPPeerChild is forked.")
    val port = args(0).toInt
    val instanceCount = args(1).toInt
    val config = toConfig(port) 
    Args(port, instanceCount, config)
  }

  @throws(classOf[Throwable])
  def main(args: Array[String]) {
    val defaultConf = new HamaConfiguration()
    val arguments = toArgs(args)
    val system = ActorSystem("BSPPeerSystem%s".format(arguments.instanceCount), 
                             arguments.config)
    defaultConf.setInt("bsp.child.instance.count", arguments.instanceCount)
    system.actorOf(Props(classOf[BSPPeerChild], defaultConf), 
                   "bspPeerChild%s".format(arguments.instanceCount))
  }
}

/**
 * Launched BSP actor via forked process.
 */
class BSPPeerChild(conf: HamaConfiguration) extends LocalService 
                                            with RemoteService {

   val taskManagerInfo = 
     new ProxyInfo.Builder().withConfiguration(conf).
                             withActorName("taskManager").
                             appendRootPath("groomServer"). // TODO: from conf
                             appendChildPath("taskManager").
                             buildProxyAtGroom
   val taskManagerPath = taskManagerInfo.getPath
   var taskManager: ActorRef = _
   var cancellable: Cancellable = _

   override def configuration: HamaConfiguration = conf

   override def name: String = 
     "bspPeerChild%s".format(conf.getInt("bsp.child.instance.count", 1))

   def request(target: ActorRef, message: Any): Cancellable = {
     import context.dispatcher
     context.system.scheduler.schedule(0.seconds, 2.seconds, target, message)
   }
 
   override def initializeServices {
     lookup("taskManager", taskManagerPath)
   }

   override def afterLinked(proxy: ActorRef) {
     taskManager = proxy
     taskManager ! Setup
   }

   def processTask: Receive = {
     case task: Task => {
       LOG.info("Start processing task {}", task.getId)
     }
   }

   override def receive = processTask orElse isProxyReady orElse timeout orElse unknown
}

