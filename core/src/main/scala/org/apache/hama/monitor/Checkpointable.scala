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
package org.apache.hama.monitor

import org.apache.hadoop.io.Writable
import org.apache.hama.logging.CommonLog
/*
import org.apache.hama.message.BSPMessageBundle
import org.apache.hama.ProxyInfo
*/
import org.apache.hama.message.MessageManager
import org.apache.hama.message.MessageView

trait Checkpointable extends CommonLog {

/*
  protected def savePeerBundle[M <: Writable](pack: Option[Pack],
                                              taskAttemptId: String,
                                              superstepCount: Long,
                                              peer: ProxyInfo,
                                              bundle: BSPMessageBundle[M]) =
    pack match {
      case None =>
        LOG.debug("No checkpointer found for TaskAttemptID "+taskAttemptId+
                  " at "+ superstepCount)
      case Some(pack) => pack.ckpt match {
        case Some(found) => found ! SavePeerMessages[M](peer, bundle)
        case None => 
          LOG.debug("No checkpointer for TaskAttemptID "+ taskAttemptId+" at "+
                    superstepCount)
      }
    }

  protected def saveSuperstep(pack: Option[Pack]) = pack match {
    case Some(pack) => pack.ckpt match {
      case Some(ckpt) => {
        val className = pack.superstep.getClass.getName
        val variables = pack.superstep.getVariables
        ckpt ! SaveSuperstep(className, variables)
      }
      case None => LOG.warning("Checkpointer not found!")
    }
    case None => LOG.warning("Checkpointer not found!")
  }
 
  protected def noMoreBundle(pack: Option[Pack],
                             taskAttemptId: String,
                             superstepCount: Long) = pack match {
    case None => LOG.warning("Checkpointer for "+taskAttemptId+" at "+
                          superstepCount+" is missing!")
    case Some(pack) => pack.ckpt match {
      case Some(found) => found ! NoMoreBundle
      case None => LOG.warning("Checkpointer for "+taskAttemptId+" at "+
                            superstepCount+" is missing!")
    }
  }
*/

  protected def checkpoint[M <: Writable](messenger: MessageManager[M],
                                          optionPack: Option[Pack]) =
    messenger.isInstanceOf[MessageView] match {
      case true => messenger.asInstanceOf[MessageView].localMessages match {
        case Some(allMsgs) => optionPack.map( (pack) =>
          pack.ckpt.map( (found) =>
            found ! Checkpoint(pack.variables, pack.nextSuperstep, allMsgs)
          )
        )
        case None => LOG.warning("No messages can be checkpointed!")
      }
      case false => LOG.warning("Messenger is not an instance of MessageView!")
    }


}


