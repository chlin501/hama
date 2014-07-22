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

import java.io.IOException

import org.apache.hadoop.io.Writable
import org.apache.hadoop.util.ReflectionUtils
import org.apache.hama.logging.Logger
import org.apache.hama.HamaConfiguration
import org.apache.hama.sync.SyncException
import scala.util.Failure
import scala.util.Success
import scala.util.Try

object SuperstepBSP {

  def apply(): SuperstepBSP = new SuperstepBSP

}

/**
 * This class has all superstep and routes through supersteps, started from the
 * first superstep, according to the execution instruction.
 */
protected class SuperstepBSP extends BSP with Logger {

  protected[v2] var supersteps = Map.empty[String, Superstep] 

  protected[v2] def getConf(peer: BSPPeer): HamaConfiguration = 
    peer.configuration
  
  @throws(classOf[IOException])
  @throws(classOf[SyncException])
  override def setup(peer: BSPPeer) { 
    val classes = getConf(peer).get("hama.supersteps.class")
    val classNames = classes.split(",")
    LOG.info(classNames.length+" superstep classes, including "+classes) 
    classNames.foreach( className => {
      LOG.debug("Instantiate "+className)
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
    Try(ReflectionUtils.newInstance(Class.forName(className), 
                                    getConf(peer)).asInstanceOf[Superstep])
  

  @throws(classOf[IOException])
  @throws(classOf[SyncException])
  override def bsp(peer: BSPPeer) {
    // TODO: when sync(), record 
    //       1. (non-shared) message 
    //       2. current supestep class
    //       3. variables map  
    findThenExecute(classOf[FirstSuperstep].getName, 
                    peer,
                    Map.empty[String, Writable]) 
  }

  protected def findThenExecute(className: String, 
                                peer: BSPPeer,
                                variables: Map[String, Writable]) {
    supersteps.find(entry => {
      if(classOf[FirstSuperstep].getName.equals(className)) {
        entry._2.isInstanceOf[FirstSuperstep]
      } else {
        entry._1.equals(className)
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
             peer.sync
             findThenExecute(clazz.getName, peer, superstep.getVariables)
           }
        }
      }
      case None => 
        throw new RuntimeException("Can't execute for "+className+" not found!")
    }
  }

  protected def eventually(peer: BSPPeer) = cleanup(peer) 

  @throws(classOf[IOException])
  override def cleanup(peer: BSPPeer) { 
    supersteps.foreach{ case (key, value) => {
      value.cleanup(peer)  
    }}
  }

}
