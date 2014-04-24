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

import akka.actor.ActorSystem
import akka.actor.ActorRef

/**
 * Tester wraps {@link org.apache.hama.HamaConfiguration} and 
 * {@link akka.TestKit.TestProbe#ref} for subclass reference.
 */
abstract class Tester(conf: HamaConfiguration, ref: ActorRef) { 

  protected var probeRef: ActorRef = _

  /**
   * {@link akka.TestKit.TestProbe.ref} 
   */
  protected def tester: ActorRef = probeRef

  /**
   * Configuration passed in from {@link TestEnv}. That should contain all 
   * testing setting.
   */
  protected def testConfiguration: HamaConfiguration = conf 
}
