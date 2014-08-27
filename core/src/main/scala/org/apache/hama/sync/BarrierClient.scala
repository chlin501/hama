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
package org.apache.hama.sync

import org.apache.hama.bsp.v2.Task
import org.apache.hama.HamaConfiguration

trait BarrierClient {

// TODO: magnet pattern
  def configureForBarrier(conf: HamaConfiguration, task: Option[Task], 
                          host: String, port: Int): 
      Option[PeerSyncClient] = task match { 
    case Some(found) => {
      val syncClient = SyncServiceFactory.getPeerSyncClient(conf)
      syncClient.init(conf, found.getId.getJobID, found.getId)
      syncClient.register(found.getId.getJobID, found.getId, host, port)
      Some(syncClient)
    }
    case None => None
  }

}
