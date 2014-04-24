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
import org.apache.zookeeper.data.Stat

trait Curator extends Conversion {

  var curatorFramework: CuratorFramework = _

  private def build(servers: String, sessionTimeout: Int, n: Int,
                    delay: Int): CuratorFramework = {
    CuratorFrameworkFactory.builder.sessionTimeoutMs(sessionTimeout).
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
    log(("Properties for ZooKeeper connection: connectString: %s,"+
        "sessionTimeout: %s, retriesN: %s, sleepBetweenRetires: %s.").
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

  /**
   * Recursively create znode.
   * The znode must be absolute path.
   * @param znode denote the path to be created in ZooKeeper.
   */
  def create(znode: String) = {
    if(null == znode || znode.isEmpty || !znode.startsWith("/"))
      throw new IllegalArgumentException("Invalid znode "+znode)
    val nodes = znode.split("/").drop(1)
    var p = ""
    nodes.foreach( node => {
      p += "/"+node
      curatorFramework.checkExists.forPath(p) match {
        case stat: Stat =>
        case _ => curatorFramework.create.forPath(p)
      }
    })
  }

  /**
   * Set data at znode with corresponded value.
   * @param znode the absolute path where data to be set.
   * @param value is the data to be set. 
   */
  def set(znode: String, value: Array[Byte]) = 
    curatorFramework.setData.forPath(znode, value) 

  protected def getBytes(znode: String): Option[Array[Byte]] = {
    curatorFramework.getData.forPath(znode) match {
      case data: Array[Byte] => Some(data)
      case _ => None
    }
  }

  /**
   * Get bytes with default value.
   * @param znode denotes the absolute path in ZooKeeper.
   * @param defaultValue is applied if not found.
   * @return Array[Byte] if found at znode; otherwise returns defaultValue.
   */
  def getBytes(znode: String, defaultValue: Array[Byte]): Array[Byte] = {
    getBytes(znode) match {
      case Some(bytes) => bytes
      case None => defaultValue
    }
  }

  /**
   * Get string with default value.
   * @param znode denotes the absolute path in ZooKeeper.
   * @param defaultValue is applied if not found.
   * @return String if found at znode; otherwise returns defaultValue.
   */
  def get(znode: String, defaultValue: String): String = {
    getBytes(znode) match {
      case Some(bytes) => new String(bytes)
      case None => defaultValue 
    }
  }

  /**
   * Get int with default value.
   * @param znode denotes the absolute path in ZooKeeper.
   * @param defaultValue is applied if not found.
   * @return Int if found at znode; otherwise returns defaultValue.
   */
  def getInt(znode: String, defaultValue: Int): Int = {
    getBytes(znode) match {
      case Some(bytes) => toInt(bytes)
      case None => defaultValue
    }
  }

  /**
   * Get float with default value.
   * @param znode denotes the absolute path in ZooKeeper.
   * @param defaultValue is applied if not found.
   * @return Float if found at znode; otherwise returns defaultValue.
   */
  def getFloat(znode: String, defaultValue: Float): Float = {
    getBytes(znode) match {
      case Some(bytes) => toFloat(bytes)
      case None => defaultValue
    }
  }

  /**
   * Get long with default value.
   * @param znode denotes the absolute path in ZooKeeper.
   * @param defaultValue is applied if not found.
   * @return Long if found at znode; otherwise returns defaultValue.
   */
  def getLong(znode: String, defaultValue: Long): Long = {
    getBytes(znode) match {
      case Some(bytes) => toLong(bytes)
      case None => defaultValue
    }
  }

  /**
   * Get double with default value.
   * @param znode denotes the absolute path in ZooKeeper.
   * @param defaultValue is applied if not found.
   * @return Double if found at znode; otherwise returns defaultValue.
   */
  def getDouble(znode: String, defaultValue: Double): Double = {
    getBytes(znode) match {
      case Some(bytes) => toDouble(bytes)
      case None => defaultValue
    }
  }
  
}
