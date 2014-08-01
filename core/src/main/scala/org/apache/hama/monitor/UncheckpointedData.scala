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
import org.apache.hama.ProxyInfo
import org.apache.hama.message.BSPMessageBundle

protected final case class PeerWithBundle[M <: Writable](
  peer: ProxyInfo, bundle: BSPMessageBundle[M]
)

trait UncheckpointedData {

  type SuperstepCount = Long

  var unckptdata = Map.empty[SuperstepCount, Seq[PeerWithBundle[Writable]]]

  def collect[M <: Writable](count: SuperstepCount, 
                             peer: ProxyInfo, 
                             bundle: BSPMessageBundle[M]) {
    unckptdata.get(count) match {
      case Some(found) => {
        val newSeq = found ++ Seq(PeerWithBundle[Writable](peer, 
        bundle.asInstanceOf[BSPMessageBundle[Writable]]))
        unckptdata ++= Map(count -> newSeq)
      }
      case None => unckptdata ++= Map(count -> 
        Seq(PeerWithBundle[Writable](peer, 
        bundle.asInstanceOf[BSPMessageBundle[Writable]])))

    }
  }

  def getUncheckpointedData(): 
    Map[SuperstepCount, Seq[PeerWithBundle[Writable]]] = unckptdata
 
  
}
