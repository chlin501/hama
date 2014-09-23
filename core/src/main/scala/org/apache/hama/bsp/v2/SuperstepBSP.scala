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
/*
import akka.actor.ActorRef
import org.apache.hadoop.io.Writable
import org.apache.hadoop.util.ReflectionUtils
import org.apache.hama.Agent
import org.apache.hama.HamaConfiguration
import org.apache.hama.monitor.Checkpointer
import org.apache.hama.monitor.CheckpointerReceiver
import org.apache.hama.monitor.Pack
import org.apache.hama.sync.SyncException
import org.apache.hama.logging.TaskLog
import scala.util.Failure
import scala.util.Success
import scala.util.Try

 * This class manages all superstep and supersteps routing, started from the
 * first superstep, according to the execution instruction.
protected class SuperstepBSP(conf: HamaConfiguration,  
                             task: Task,
                             container: ActorRef,
                             coordinator: ActorRef,
                             tasklog: ActorRef) extends Agent {

  protected[v2] var supersteps = Map.empty[String, ActorRef] 

  protected def setup: Receive = {
    case Setup => {
      begingOfSetup
    }
  }

  protected def begingOfSetup() {
    task.markTaskStarted
    task.transitToSetup 
    task.markAsRunning 
  }

  protected def whenSetup() {
    val taskConf = task.getConfiguration
    val classes = taskConf.get("hama.supersteps.class")
    LOG.info("Supersteps to be instantiated include {}", classes)
    val classNames = classes.split(",")
    classNames.foreach( className => {
      instantiate(className, taskConf) match {
        case Success(instance) => { 
          //instance.setup(peer)
          supersteps ++= Map(className -> instance)
        }
        case Failure(cause) => 
          throw new IOException("Fail instantiating "+className, cause)
      }
    })  
  }

  protected def instantiate(className: String, 
                            taskConf: HamaConfiguration): Try[Superstep] = 
    Try(ReflectionUtils.newInstance(classWithLoader(className), taskConf).
                        asInstanceOf[Superstep])

  @throws(classOf[IOException])
  @throws(classOf[SyncException])
  override def setup(peer: BSPPeer) {
    beginOfSetup(peer)
    whenSetup(peer)
    endOfSetup(peer)
  }

   * Load class from a particular load which contains the target class.
   * @param className is the name of class to be loaded.
   * @return Class to be instantiated.
  protected def classWithLoader(className: String): Class[_] = 
    TaskOperator.execute[Class[_]](taskOperator, { (task) => 
      Class.forName(className, true, task.getConfiguration.getClassLoader)
    }, null.asInstanceOf[Class[_]])


  @throws(classOf[IOException])
  @throws(classOf[SyncException])
  override def bsp(peer: BSPPeer) {
    beginOfBSP(peer)
    whenBSP(peer)
    endOfBSP(peer)
  }

  // TODO: check if it's a resume task; if true replace FirstSuperstep with checkpointed one; otherwise start from first superstep!
  override def whenBSP(peer: BSPPeer) = 
    findThenExecute(classOf[FirstSuperstep].getName, peer, 
                    Map.empty[String, Writable]) 

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
        beforeCompute(peer, superstep)
        whenCompute(peer, superstep)
        afterCompute(peer, superstep)
        val next = superstep.next
        next match { // TODO: null==next causes sync is not executed. check if need to change to sync before going to cleanup!
           case null => eventually(peer)
           case clazz@_ => {
             beforeSync(peer, superstep)
             whenSync(peer, superstep)
             afterSync(peer, superstep)
             findThenExecute(clazz.getName, peer, superstep.getVariables)
           }
        }
      }
      case None => 
        throw new RuntimeException("Can't execute for "+className+" not found!")
    }
  }

  override def beforeSync(peer: BSPPeer, superstep: Superstep) = 
    prepareForCheckpoint(peer, superstep)

  override def beforeCompute(peer: BSPPeer, superstep: Superstep) = 
    TaskOperator.execute(taskOperator, { (task) => {
      task.transitToCompute 
      reporter ! task.toStat
    }})
  

  override def whenCompute(peer: BSPPeer, superstep: Superstep) = 
    superstep.compute(peer)

  override def whenSync(peer: BSPPeer, superstep: Superstep) = peer.sync

  protected def createCheckpointer(peer: BSPPeer): Option[ActorRef] = 
    TaskOperator.execute[Option[ActorRef]](taskOperator, { (task) => {
      val superstepCount = peer.getSuperstepCount
      val actorName = "checkpoint-"+task.getId.toString+"-"+superstepCount
      val ckpt = spawn(actorName, classOf[Checkpointer], commonConf(peer), 
                       task.getConfiguration, task.getId.toString, 
                       superstepCount)
      LOG.debug("Checkpointer "+ckpt.path.name+" is created!")
      Some(ckpt)
    }}, None)

  protected[v2] def prepareForCheckpoint(peer: BSPPeer, superstep: Superstep): 
    Option[ActorRef] = isCheckpointEnabled(commonConf(peer)) match {
      case true => {
        val ckpt = createCheckpointer(peer)
        peer.isInstanceOf[CheckpointerReceiver] match {
          case true => peer.asInstanceOf[CheckpointerReceiver].
                            receive(Pack(ckpt, superstep.getVariables, 
                                         superstep.next))
          case false => LOG.warning("Checkpointer is created, but BSPPeer is "+
                                    "not an instance of CheckpointerReceiver!")
        }
        ckpt
      }
      case false => None
    }

   * Check common configuration if checkpoint is needed.
   * @param conf is the common configuration.
  // TODO: move to checkpoint related api
  protected def isCheckpointEnabled(conf: HamaConfiguration): Boolean = 
    conf.getBoolean("bsp.checkpoint.enabled", true)

  protected def eventually(peer: BSPPeer) = cleanup(peer) 

  override def beginOfCleanup(peer: BSPPeer) = 
    TaskOperator.execute(taskOperator, { (task) => {
      task.transitToCleanup 
      reporter ! task.toStat
    }})

  override def endOfCleanup(peer: BSPPeer) = 
    TaskOperator.execute(taskOperator, { (task) => { 
      task.markAsSucceed 
      task.markTaskFinished
      reporter ! task.toStat
    }})

  override def whenCleanup(peer: BSPPeer) = supersteps.foreach { 
    case (key, value) => {
      value.cleanup(peer)  
  }}

  @throws(classOf[IOException])
  override def cleanup(peer: BSPPeer) { 
    beginOfCleanup(peer)
    whenCleanup(peer)
    endOfCleanup(peer)
  }

  override def receive = setup orElse unknown
  
}
*/
