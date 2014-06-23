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
package org.apache.hama.fs

import org.apache.hama.HamaConfiguration
import org.apache.hama.pipes.util.DistributedCacheUtil

/**
 * Intended to be a wrapper for decoupling from underlying cache services so
 * that a neutral mechanism can be provided.
 */
trait CacheService {

  /**
   * Move cached files to local file system.
   * @param conf should contains all cached files setting.
   */
  def moveCacheToLocal(conf: HamaConfiguration) 

}

object CacheService {

  def moveCacheToLocal(conf: HamaConfiguration) = 
    DistributedCacheUtil.moveLocalFiles(conf)
  
}
