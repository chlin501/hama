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

final case class Args(port: Int, seq: Int, config: Config)

object BSPPeerContainer {

  def toConfig(port: Int): Config = {
    ConfigFactory.parseString(s"""
      peer {
        akka {
          loggers = ["akka.event.slf4j.TaskLogger"]
          loglevel = "INFO"
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
                                         "BSPPeerContainer is forked.")
    val port = args(0).toInt
    val seq = args(1).toInt
    val config = toConfig(port) 
    Args(port, seq, config)
  }

  def initialize(args: Array[String]): (ActorSystem, HamaConfiguration, Int) = {
    val defaultConf = new HamaConfiguration()
    val arguments = toArgs(args)
    val system = ActorSystem("BSPPeerSystem%s".format(arguments.seq), 
                             arguments.config.getConfig("peer"))
    defaultConf.setInt("bsp.child.slot.seq", arguments.seq)
    (system, defaultConf, arguments.seq)
  }

  def launch(system: ActorSystem, conf: HamaConfiguration, seq: Int) {
    system.actorOf(Props(classOf[BSPPeerContainer], conf), 
                   "bspPeerContainer%s".format(seq))
  }

  @throws(classOf[Throwable])
  def main(args: Array[String]) = {
    val (sys, conf, seq)= initialize(args)
    launch(sys, conf, seq)
  }
}

/**
 * Launched BSP actor in forked process.
 * @param conf contains setting sepcific to this service.
 */
class BSPPeerContainer(conf: HamaConfiguration) extends LocalService 
                                                with RemoteService {

   val groomName = configuration.get("bsp.groom.name", "groomServer")
   //val executorInfo = createProxy
   //val executorPath = executorInfo.getPath
   protected var executor: ActorRef = _
   override def configuration: HamaConfiguration = conf
   def executorName: String = groomName+"_executor_"+slotSeq
   def slotSeq: Int = configuration.getInt("bsp.child.slot.seq", 1)

   // TODO: refactor for easier proxy lookup!
   protected def executorInfo: ProxyInfo = { 
     new ProxyInfo.Builder().withConfiguration(configuration).
                             withActorName(executorName).
                             appendRootPath(groomName). 
                             appendChildPath(executorName). 
                             buildProxyAtGroom
   }

   protected def executorPath: String = executorInfo.getPath

   override def name: String = "bspPeerContainer%s".format(slotSeq)
 
   override def initializeServices {
     lookup(executorName, executorPath)
   }

   override def afterLinked(proxy: ActorRef) {
     executor = proxy
     executor ! ContainerReady
     LOG.debug("Slot seq {} sends ContainerReady to {}", 
               slotSeq, executor.path.name)
   }

   /**
    * Start executing task dispatched to the container.
    * @return Receive is partial function.
    */
   def processTask: Receive = {
     case task: Task => {
       LOG.info("Start processing task {}", task.getId)
       // not yet implemented ... 
     }
   }

   /**
    * A function to close all necessary operations before shutting down the 
    * system.
    */
   protected def close { 
     LOG.debug("Stop related operations before exiting programme {} ...", name)
   }

   override def postStop {
     close
     super.postStop
   }

   /**
    * Close all related process operations and then shutdown the actor system.
    * @return Receive is partial function.
    */
   def stopContainer: Receive = {
     case StopContainer => {
       context.unwatch(executor)
       executor ! ContainerStopped  
       LOG.debug("Send ContainerStopped message ...")
     }
   }

   def shutdownSystem: Receive = {
     case ShutdownSystem => {
       LOG.info("Completely shutdown BSPContainer system ...")
       context.stop(self)
       context.system.shutdown
     }
   }

   override def offline(target: ActorRef) {
     LOG.info("{} is unwatched.", target.path.name)
   }

   override def receive = shutdownSystem orElse stopContainer orElse processTask orElse isProxyReady orElse timeout orElse superviseeIsTerminated orElse unknown
}
