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
//import org.apache.hama.bsp.v2.GroomServerSpec

class Registrator(conf: HamaConfiguration) extends LocalService 
                                              with RemoteService {

  val groomManagerInfo = 
    new ProxyInfo.Builder().withConfiguration(conf).
                            withActorName("groomManager").
                            appendRootPath("bspmaster").
                            appendChildPath("groomManager").
                            buildProxyAtMaster

  val groomManagerPath = groomManagerInfo.getPath

  val groomServerHost = configuration.get("bsp.groom.address", "127.0.0.1")
  val groomServerPort = configuration.getInt("bsp.groom.port", 50000)
  val groomServerName = "groom_"+ groomServerHost +"_"+ groomServerPort
  val maxTasks = configuration.getInt("bsp.tasks.maximum", 3)
  //val freeSlots = maxTasks

  override def configuration: HamaConfiguration = conf

  override def name: String = "registrator"

  override def initializeServices {
    lookup("groomManager", groomManagerPath)
  }
  
  override def afterLinked(proxy: ActorRef) { 
//    proxy ! new GroomServerSpec(groomServerName, 
                                //groomServerHost, 
                                //groomServerPort, 
                                //maxTasks, 
                                //freeSlots)
  }

  override def offline(target: ActorRef) {
    lookup("groomManager", groomManagerPath)
  }

  override def receive = isServiceReady orElse serverIsUp orElse isProxyReady orElse timeout orElse superviseeIsTerminated orElse unknown

}
