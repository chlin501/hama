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
import org.apache.hama.message.MessageManager
import org.apache.hama.message.MessageView

trait Checkpointable extends CommonLog {

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


