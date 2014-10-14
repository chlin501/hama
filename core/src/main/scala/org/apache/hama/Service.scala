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
package org.apache.hama

import akka.actor.ActorRef
import akka.actor.Terminated

/**
 * A service may hold a collection of local services
 */
trait Service extends Agent {

  /**
   * Logic for intantiating necessary prerequisite operations.
   */
  protected def initializeServices = {}

  /**
   * A configuration file specific to Hama system.
   */
  protected def configuration: HamaConfiguration // TODO: remove this?

  /**
   * Initialize necessary subservices.
   */
  override def preStart = initializeServices 

  /**
   * Notify the target service is offline.
   * @param target that may be offline.  
   */
  protected def offline(target: ActorRef) { }

  /**
   * This function is executed before actor is restarted.
   * @param what causes this restart.
   * @param msgReceived identifies during which message execution leads to the
   *        restart.
   */
  protected def beforeRestart(what: Throwable, msgReceived: Option[Any]) { }

  /**
   * This function is executed after actor is restarted.
   * @param what causes this restart.
   */
  protected def afterRestart(what: Throwable) { }

  override def preRestart(reason: Throwable, message: Option[Any]) =  
    beforeRestart(reason, message)

  override def postRestart(reason: Throwable) =  afterRestart(reason)

  /**
   * when target service is offline.
   */
  protected def superviseeIsTerminated: Receive = {
    case Terminated(target) => offline(target)
  }

  /**
   * Stop services needed.
   */
  protected def stopServices = {}

  override def postStop = stopServices

}
