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
package org.apache.hama.util

import akka.event.Logging
import org.apache.hama.Agent
import org.apache.hama.HamaConfiguration
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier
import org.apache.curator.retry.RetryNTimes

trait Curator {

  var curatorFramework: CuratorFramework = _

  private def build(servers: String, timeout: Int, n: Int,
                    delay: Int): CuratorFramework = {
    CuratorFrameworkFactory.builder.connectionTimeoutMs(timeout).
                            retryPolicy(new RetryNTimes(n, delay)).
                            connectString(servers).build
  }

  /**
   * Initialize curator instance.
   * @param conf contains information for connecting to ZooKeeper, including
   *             connectString, sessionTimeout, retriesN, and 
   *             sleepBetweenRetries.
   */
  def initializeCurator(conf: HamaConfiguration) {
    val connectString =
      conf.get("hama.zookeeper.property.connectString", "localhost:2181")
    val sessionTimeout = conf.getInt("hama.zookeeper.session.timeout",
                                     3*60*1000)
    val retriesN = conf.getInt("bsp.zookeeper.client.retry_n_times", 10)
    val sleepBetweenRetries =
      conf.getInt("bsp.zookeeper.client.sleep_between_delay", 1000)
    log("Properties for ZooKeeper connection -> connectString: %s,"+
        "sessionTimeout: %s, retriesN: %s, sleepBetweenRetires: %s.".
        format(connectString, sessionTimeout, retriesN, sleepBetweenRetries))
    curatorFramework = build(connectString, sessionTimeout,
                             retriesN, sleepBetweenRetries)
    curatorFramework.start
    log("CuratorFramework is started!")
  }

  /**
   * Abstract method for log message.
   * @param message to be logged.
   */
  def log(message: String)
}
