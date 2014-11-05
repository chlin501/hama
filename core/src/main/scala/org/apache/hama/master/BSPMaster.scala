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
package org.apache.hama.master

import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.actor.Props
//import akka.cluster.Cluster
//import akka.cluster.ClusterEvent.MemberUp
//import akka.cluster.ClusterEvent.UnreachableMember
//import akka.cluster.ClusterEvent.MemberRemoved
//import akka.cluster.ClusterEvent.MemberEvent
import org.apache.hama.HamaConfiguration
import org.apache.hama.SystemInfo
import org.apache.hama.conf.Setting
import org.apache.hama.ServiceStateMachine
import org.apache.hama.util.Curator
import org.apache.zookeeper.CreateMode
import scala.concurrent.duration.DurationInt
import scala.concurrent.duration.FiniteDuration

object BSPMaster {

  def main(args: Array[String]) {
    // TODO: get seed nodes from zk  (another helper class get/post from/to zk?)
    //       post self as seed node info to zk
    //       combine all seed nodes as indexedSeq and put into setting
    val master = Setting.master
    val registrator = new MasterRegistrator(master)
    registrator.register
    val sys = ActorSystem(master.info.getActorSystemName, master.config)
    sys.actorOf(Props(classOf[BSPMaster], master), 
                master.name)
  }
 
}

// TODO: member management trait for harness groom registration, leave, etc.
//       BSPMaster extends member management trait
//       refactor FSM (perhaps remove it)
class BSPMaster(setting: Setting) extends ServiceStateMachine {

  //val cluster = Cluster(context.system)

  import BSPMaster._

  override def initializeServices {
    // TODO: member management trait
    // cluster.joinSeedNodes(...)
    // cluster.subscribe ...
    val receptionist = getOrCreate("receptionist", classOf[Receptionist], 
                                   setting.hama) 
    getOrCreate("monitor", classOf[Monitor], setting.hama) 
    val sched = getOrCreate("sched", classOf[Scheduler], setting.hama, 
                            receptionist) 
    getOrCreate("groomManager", classOf[Scheduler], setting.hama, 
                receptionist, sched) 
  }

  //override def postStop = cluster.unsubscribe

  override def receive = serviceStateListenerManagement orElse super.receive orElse unknown
  
}

class MasterRegistrator(setting: Setting) extends Curator {

  initializeCurator(setting.hama)

  def register() {
    val sys = setting.info.getActorSystemName
    val host = setting.info.getHost
    val port = setting.info.getPort
    val path = "/%s/%s_%s@%s:%s".format("masters", setting.name, sys, host, 
                                        port)
    create(path) 
  }

  def masters(): Array[SystemInfo] = list("/masters").map { child => {
    LOG.debug("Master znode found is {}", child)
    val ary = child.split("_")
    val name = ary(0)
    val ary1 = ary(1).split("@")
    val sys = ary1(0) 
    val ary2 = ary1(1).split(":")
    val host = ary2(0)
    val port = ary2(1)
    new SystemInfo(sys, host, port.toInt)
  }}.toArray

}
