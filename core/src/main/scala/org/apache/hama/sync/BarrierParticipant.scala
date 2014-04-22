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

import org.apache.hama.HamaConfiguration
import org.apache.hama.util.Curator
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier

/**
 * This class runs on forked process, so it's dedicated to a single task
 * computation.
 */
trait BarrierParticipant extends Participant with Curator {

  protected var rootPath: String = _
  protected var meetingPlace: String = _
  protected var barrier: DistributedDoubleBarrier = _

  private def validate(jobId: String, superstep: Long, totalTasks: Int) {
    if(null == jobId || "".equals(jobId))
      throw new IllegalArgumentException("Invalid BSPJobID.");

    if(0 >= superstep)
       throw new IllegalArgumentException("Invalid superstep: "+superstep)

    if(0 >= totalTasks)
      throw new IllegalArgumentException("Invalid totalTasks: "+totalTasks)
  }

  /**
   * Obtain the root path when a task performs barrier synchronization.
   * @param conf contains the root path setting for barrier sync.
   */
  def configure(conf: HamaConfiguration) = { 
    initializeCurator(conf)
    rootPath = conf.get("bsp.sync.barrier.root", "/bsp/sync");
  }

  /**
   * Build the barrier path in a form of <syncRootPath>/<jobId>/<superstep>.
   * @param syncRootPath denotes the root path for tasks synchronization; 
   *                     default pointed to <b>/bsp/sync</b>
   * @param jobId denotes which job involves in the barrier sync.
   * @param superstep indicates at which superstep this task is.
   */
  def barrierPath: String = {
    if(null == meetingPlace && !"".equals(meetingPlace)) 
      throw new IllegalStateException("Barrier path is not initialized.")
    meetingPlace
  }

  /**
   * Build the barrier instance of
   * {@link org.apache.curator.framework.recipe.DistributedDoubleBarrier}.
   *
   * When a task is executing at different superstep, the barrier instance 
   * will be changed because the rendezvous path can only be constructed 
   * during object instantiation.
   *
   * @param jobId denotes which job involves in the barrier sync.
   * @param superstep indicates at which superstep this task is.
   * @param syncPath denotes the entire barier synchronization path, started
   *                 from <b>syncRootPath</b>.
   * @param totalTasks indicates the total tasks will involves in barrier 
   *                   synchronization.
   */
  def build(jobId: String, superstep: Long, totalTasks: Int) {
    validate(jobId, superstep, totalTasks)
    meetingPlace = "%s/%s/%s".format(rootPath, jobId, superstep)
    log("Initialize barrier path at %s".format(meetingPlace))
    if(null == curatorFramework)
      throw new IllegalStateException("CuratorFramework is not initialized.")
    barrier = new DistributedDoubleBarrier(curatorFramework, 
                                           meetingPlace, totalTasks) 
  }

  /**
   * Enter the barrier synchronization.
   */
  override def enter = {
    if(null == barrier) 
      throw new IllegalStateException("Barrier object not found.")
    barrier.enter
  }

  /**
   * Leave the barrier synchronization.
   */
  override def leave = {
    if(null == barrier) 
      throw new IllegalStateException("Barrier object not found.")
    barrier.leave 
  }

}
