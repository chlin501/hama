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
package org.apache.hama.monitor.master

import akka.actor.ActorRef
import org.apache.hama.Agent
import org.apache.hama.HamaConfiguration
import org.apache.hama.bsp.BSPJobID
import org.apache.hama.fs.Operation
import org.apache.hama.util.Curator
import org.apache.hama.monitor.Tracker

final class Checker(conf: HamaConfiguration,  // common conf
                    jobId: BSPJobID,
                    superstep: Int, 
                    totalTasks: Int) extends Agent with Curator {

  val operation = Operation.get(conf)
  
  override def preStart() = {
    initializeCurator(conf)
    // TODO: 1. mk checkpoint path in hdfs. 
    //       2. check integrity in hdfs.
    //       3. if all checkpoints (equals to total tasks) exist and every 
    //          checkpoint has complete images such as msgs, superstep, 
    //          then write ok to zk with cooresponded path 
    //          e.g. jobid/superstep.ok
    //       4. once finishing, call stop
  }

  private def close() = stop

  override def receive = unknown

}

object CheckpointIntegrator {

  val superstepOf = "superstep-of-"

}

final class CheckpointIntegrator extends Tracker {

  import CheckpointIntegrator._

  private var children = Set.empty[ActorRef]

  override def initialize() = subscribe(SuperstepIncrementEvent) 

  override def notified(msg: Any) = msg match {
    case LatestSuperstep(jobId, n, totalTasks) => {
      children += spawn(superstepOf + n, classOf[Checker], 
                        configuration, jobId, n, totalTasks)
    } 
    case _ => LOG.warning("Unknown message {}!", msg)
  }
}

