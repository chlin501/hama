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
import org.apache.hama.sync.SyncException

/**
 * This class is the base class that perform bsp computation.
 */
abstract class BSP {

  /**
   * This function is executed before bsp().
   * @throws IOException is thrown when io fails.
   * @throws SyncException is thrown when barrier sync fails.
   */
  @throws(classOf[IOException])
  @throws(classOf[SyncException])
  def setup(peer: BSPPeer) { }

  /**
   * The main function that performs bsp computation.
   * @throws IOException when io not functions correctly.
   * @throws SyncException is thrown when barrier sync fails.
   */
  @throws(classOf[IOException])
  @throws(classOf[SyncException])
  def bsp(peer: BSPPeer) 

  /**
   * This function is executed after bsp(). 
   * @throws IOException is thrown when io fails.
   */
  @throws(classOf[IOException])
  def cleanup(peer: BSPPeer) { }

}
