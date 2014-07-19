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

/**
 * The main place to programme BSP logic.
 * Each superstep should be executed accoring to the routing map proposed.
 */
abstract class Superstep {

  /**
   * BSP main logic to be executed.
   * @param peer is a {@link BSPPeer} objet containing necessary services
   *             during computation.
   */
  def compute(peer: BSPPeer) 

  /**
   * The returned class points the to next {@link Superstep} to be executed.
   * @return Class of {@link Superstep} type.
   */
  def next: Class[_ <: Superstep]
}
