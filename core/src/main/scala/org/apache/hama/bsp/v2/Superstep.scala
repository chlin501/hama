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

import org.apache.hadoop.io.Writable

/**
 * The main place to programme BSP logic.
 * Each superstep should be executed accoring to the routing map proposed.
 */
abstract class Superstep {

  /**
   * This variable will be saved to external storage so it can be restored.
   */
  protected var variables = Map.empty[String, Writable]

  /**
   * Obtain value with cooresponded key provided in the previous supersteps.
   * @param key that identifies the value. 
   * @return Writable may contains null if no data found.
   */
  def find[W <: Writable](key: String): W = 
    variables.getOrElse(key, null).asInstanceOf[W]

  /**
   * Collect key value pair for future supersteps.
   * @param key is used to identifies the value.
   * @param value is the actual data.
   */
  def collect[W <: Writable](key: String, value: W) = 
    variables ++= Map(key -> value)

  /**
   * Obtain all variables that is preserved between supersteps.
   * This is intended to be used by {@link SuperstepBSP} only.
   * @return Map that contains all variables.
   */
  def getVariables(): Map[String, Writable] = variables

  /**
   * Set all variables. This is intended to be called by {@link SuperstepBSP}.
   * Intended to be used by {@link SuperstepBSP} only.
   * @param variables of all keys and values.
   */
  protected[v2] def setVariables(variables: Map[String, Writable]) = 
    this.variables = variables

  /**
   * Prepare before starting computation.
   * @param peer is {@link BSPPeer} holds services such as sync, messeging, etc.
   */
  def setup(peer: BSPPeer) { }

  /**
   * BSP main logic to be executed.
   * @param peer is a {@link BSPPeer} objet containing necessary services
   *             during computation.
   */
  def compute(peer: BSPPeer) 

  /**
   * Cleanup after computation is finished.
   * @param peer is {@link BSPPeer} holds services such as sync, messeging, etc.
   */
  def cleanup(peer: BSPPeer) { }


  /**
   * The returned class points the to next {@link Superstep} to be executed.
   * @return Class of {@link Superstep} type.
   */
  def next: Class[_ <: Superstep]
}
