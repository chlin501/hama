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
package org.apache.hama.bsp.v2

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.Props
import java.io.IOException
import org.apache.hadoop.conf.Configurable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Writable
import org.apache.hadoop.util.ReflectionUtils
import org.apache.hama.Agent
import org.apache.hama.HamaConfiguration
import org.apache.hama.monitor.Checkpointer
import org.apache.hama.monitor.CheckpointerReceiver
import org.apache.hama.monitor.Pack
import org.apache.hama.sync.SyncException
import org.apache.hama.logging.TaskLog
import org.apache.hama.logging.TaskLogParam
import org.apache.hama.logging.TaskLogParameter
import scala.util.Failure
import scala.util.Success
import scala.util.Try

object SuperstepBSP {

  val tasklogsPath = "/logs/taskslogs"

}

/**
 * This class manages all superstep and supersteps routing, started from the
 * first superstep, according to the execution instruction.
 */
protected trait SuperstepBSP extends BSP 
                             with Agent 
                             with TaskLog 
                             with TaskLogParameter
                             with Configurable {
 
  //self: Actor with TaskLog with TaskParameter =>

  import SuperstepBSP._

  protected[v2] var supersteps = Map.empty[String, Superstep] 

  protected[v2] var task: Option[Task] = None

  //protected def getTask(): Task // TODO: doIfExists

  override def getTaskLogParam(): TaskLogParam = 
    TaskLogParam(context.system, getLogDir(hamaHome), slotSeq) 

  // TODO: check if any better way to set hama home. 
  protected def hamaHome: String = System.getProperty("hama.home.dir")

  protected def getLogDir(hamaHome: String): String = hamaHome+tasklogsPath

  protected def slotSeq: Int //= conf.getInt("bsp.child.slot.seq", -1)

  /**
   * This is configuration for a specific task. 
   */
  protected var taskConf = new HamaConfiguration

  override def setConf(conf: Configuration) = conf match {
    case null => //LOG.error("Task configuration is not provided for {}!",
                  //         getTask.getId) 
    case _ => this.taskConf = conf.asInstanceOf[HamaConfiguration]
  }

  override def getConf(): Configuration = this.taskConf 

  /**
   * This function returns common configuration from BSPPeer.
   * @param peer for the aggregation of all supersteps.
   * @return HamaConfiguration is common configuration, not specific to any 
   *                           tasks.
   */
  protected[v2] def commonConf(peer: BSPPeer): HamaConfiguration = 
    peer.configuration
  
  @throws(classOf[IOException])
  @throws(classOf[SyncException])
  override def setup(peer: BSPPeer) { 
    val classes = commonConf(peer).get("hama.supersteps.class")
    //LOG.info("Supersteps to be instantiated include {}", classes)
    val classNames = classes.split(",")
    //LOG.info(classNames.length+" superstep classes, including "+classes) 
    classNames.foreach( className => {
      //LOG.debug("Instantiate "+className)
      instantiate(className, peer) match {
        case Success(instance) => { 
          instance.setup(peer)
          supersteps ++= Map(className -> instance)
        }
        case Failure(cause) => 
          throw new IOException("Fail instantiating "+className, cause)
      }
    })
  }

  protected def instantiate(className: String, peer: BSPPeer): Try[Superstep] = 
    Try(ReflectionUtils.newInstance(Class.forName(className, 
                                                  true, 
                                                  taskConf.getClassLoader), 
                                    commonConf(peer)).asInstanceOf[Superstep])
  

  @throws(classOf[IOException])
  @throws(classOf[SyncException])
  override def bsp(peer: BSPPeer) {
    // TODO: check if it's a resume task; if true replace FirstSuperstep with checkpointed one; otherwise start from first superstep!
    findThenExecute(classOf[FirstSuperstep].getName, 
                    peer,
                    Map.empty[String, Writable]) 
  }

  protected def findThenExecute(className: String, 
                                peer: BSPPeer,
                                variables: Map[String, Writable]) {
    supersteps.find(entry => {
      if(classOf[FirstSuperstep].getName.equals(className)) {
        val classInstance = entry._2
        classInstance.isInstanceOf[FirstSuperstep]
      } else {
        val classKey = entry._1
        classKey.equals(className)
      }
    }) match {
      case Some(found) => {
        val superstep = found._2
        superstep.setVariables(variables)
        superstep.compute(peer)
        val next = superstep.next
        next match {
           case null => eventually(peer)
           case clazz@_ => {
             prepareForCheckpoint(peer, superstep)
             peer.sync
             findThenExecute(clazz.getName, peer, superstep.getVariables)
           }
        }
      }
      case None => 
        throw new RuntimeException("Can't execute for "+className+" not found!")
    }
  }

  protected def createCheckpointer(peer: BSPPeer, 
                                   superstep: Superstep): Option[ActorRef] = 
      task match { 
    case Some(found) => {
      val superstepCount = peer.getSuperstepCount
      val taskAttemptId = found.getId
      val actorName = "checkpoint_"+taskAttemptId+"_"+superstepCount
      Some(context.actorOf(Props(classOf[Checkpointer], 
                                 taskConf, 
                                 taskAttemptId, 
                                 superstepCount), 
                           actorName))
    }
    case None => None
  }
    

  // TODO: move to checkpointer receiver?
  protected[v2] def prepareForCheckpoint(peer: BSPPeer, superstep: Superstep): 
    Option[ActorRef] = isCheckpointEnabled(commonConf(peer)) match {
      case true => {
        val ckpt = createCheckpointer(peer, superstep)
/*
        val ckpt = context.actorOf(Props(classOf[Checkpointer], 
                                         taskConf,
                                         doIfExists[Task, Unit](task, { (f) => 
                                           f.getId
                                         }, Unit)
                                         peer.getSuperstepCount), 
                                    "checkpoint_"+getTask.getId+"_"+
                                        peer.getSuperstepCount) 
*/
        //LOG.debug("Checkpoint "+ckpt.path.name+" is created!")
        peer.isInstanceOf[CheckpointerReceiver] match {
          case true => peer.asInstanceOf[CheckpointerReceiver].
                            receive(Pack(ckpt, superstep))
          case false => //LOG.warning("Checkpoint "+ckpt.path.name+" is created"+
                         //        ", but can't be assigned to BSPPeer because "+
                          //       " not an instance of CheckpointerReceiver!")
        }
        ckpt
      }
      case false => {
        //LOG.debug("No checkpoint is needed for "+getTask.getId)
        None
      }
    }

  /**
   * Check common configuration if checkpoint is needed.
   * @param conf is the common configuration.
   */
  protected def isCheckpointEnabled(conf: HamaConfiguration): Boolean = 
    conf.getBoolean("bsp.checkpoint.enabled", true)

  protected def eventually(peer: BSPPeer) = cleanup(peer) 

  @throws(classOf[IOException])
  override def cleanup(peer: BSPPeer) { 
    supersteps.foreach{ case (key, value) => {
      value.cleanup(peer)  
    }}
  }
}
