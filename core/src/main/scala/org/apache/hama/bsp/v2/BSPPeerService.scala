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

import akka.actor.ActorContext
import akka.actor.ActorRef
import akka.actor.Props
import akka.event.Logging
import scala.collection.immutable.Set

/**
 * TODO: removed?
 * used by coordinator for init sub services e.g. messeging, etc.
 */
trait BSPPeerService {

  type Service = ActorRef

  protected var services = Set.empty[Service]

  protected def actorConext: ActorConext 

  /**
   * Get or create ActorRef.
   */
  protected def getOrCreate(serviceName: String, clazz: Class[_], args: Any*): 
      ActorRef = {
    services.find(s => s.path.name.equals(serviceName)) match {
      case Some(found) => found
      case None => actorContext.actorOf(Props(clazz, args:_*))
    }
  }

}
