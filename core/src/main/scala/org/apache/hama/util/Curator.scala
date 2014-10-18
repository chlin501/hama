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
import org.apache.hama.logging.CommonLog
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.imps.CuratorFrameworkState._
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier
import org.apache.curator.retry.RetryNTimes
import org.apache.zookeeper.data.Stat

object Curator extends CommonLog {

  def build(servers: String, sessionTimeout: Int, retryN: Int,
            delay: Int): CuratorFramework =
    CuratorFrameworkFactory.builder.sessionTimeoutMs(sessionTimeout).
                                    retryPolicy(new RetryNTimes(retryN, delay)).
                                    connectString(servers).build

  def build(conf: HamaConfiguration): CuratorFramework = {
    val servers = conf.get("hama.zookeeper.property.connectString", 
                           "localhost:2181")
    val sessionTimeout = conf.getInt("hama.zookeeper.session.timeout",
                                     3*60*1000)
    val retryN = conf.getInt("bsp.zookeeper.client.retry_n_times", 10)
    val sleepBeforeRetry =
      conf.getInt("bsp.zookeeper.client.sleep_before_retry", 1000)
    LOG.info("Properties for ZooKeeper connection contain servers: {},"+
             "sessionTimeout: {}, retriesN: {}, sleepBeforeRetry: {}.",
             servers, sessionTimeout, retryN, sleepBeforeRetry)
    build(servers, sessionTimeout, retryN, sleepBeforeRetry) 
  }
}

trait Curator extends Conversion with CommonLog {

  import Curator._

  protected var curatorFramework: Option[CuratorFramework] = None

  /**
   * Initialize curator instance.
   * @param conf contains information for connecting to ZooKeeper, including
   *             connectString, sessionTimeout, retriesN, and 
   *             sleepBetweenRetries.
   */
  def initializeCurator(conf: HamaConfiguration): Option[CuratorFramework] = {
    curatorFramework = Option(build(conf))
    curatorFramework.map { (client) => 
      client.getState match {
        case LATENT => {
          LOG.info("Start CuratorFramework ...")
          client.start
        }
        case STARTED => LOG.info("CuratorFramework is started!")
        case STOPPED => {
          LOG.info("CuratorFramework is stopped! Restart it again ...")
          client.start
        }
      }
    }
    curatorFramework
  }

  protected def ifMatch[R <: Any](f: (CuratorFramework) => R): R = 
    curatorFramework match {
      case Some(client) => f(client)
      case None => throw new RuntimeException("Curator not initialized!")
    }

  /**
   * Recursively create znode. 
   * No value is set with this function.
   * The znode must be an absolute path.
   * This is not an atomic operation. 
   * Use {@link org.apache.curator.framework.api.transaction.CuratorTransaction}
   * instead if needed.
   * @param znode denote the path to be created in ZooKeeper.
   */
  def create(znode: String) {
    if(null == znode || znode.isEmpty || !znode.startsWith("/"))
      throw new IllegalArgumentException("Invalid znode "+znode)
    val nodes = znode.split("/").drop(1)
    var p = ""
    nodes.foreach( node => {
      p += "/"+node
      ifMatch({ (client) => client.checkExists.forPath(p) match {
        case stat: Stat =>
        case _ => client.create.forPath(p)
      }})
    })
  }
 
  /**
   * List all children under a particular znode.
   * @param znode contains a list of child znodes. 
   */
  def list(znode: String): Array[String] = ifMatch({ (client) => 
    client.checkExists.forPath(znode) match {
      case stat: Stat => client.getChildren.forPath(znode).toArray.
                                asInstanceOf[Array[String]]
      case _ => Array[String]()
    }
  })

  /**
   * Set data at znode with corresponded value.
   * @param znode the absolute path where data to be set.
   * @param value is the data to be set. 
   */
  def set(znode: String, value: Array[Byte]) = ifMatch({ (client) => 
    client.setData.forPath(znode, value) 
  })

  /**
   * Get bytes from the designated znode.
   * @param znode is the destination path where data is stored.
   * @return Option[Array[Byte]] is the data retrieved out of znode.
   */
  protected def getBytes(znode: String): Option[Array[Byte]] = 
    ifMatch[Option[Array[Byte]]]({ (client) =>
      client.getData.forPath(znode) match {
        case data: Array[Byte] => Option(data)
        case _ => None
      }
    })

  /**
   * Get bytes with default value.
   * @param znode denotes the absolute path in ZooKeeper.
   * @param defaultValue is applied if not found.
   * @return Array[Byte] if found at znode; otherwise returns defaultValue.
   */
  def getBytes(znode: String, defaultValue: Array[Byte]): Array[Byte] = 
    getBytes(znode) match {
      case Some(bytes) => bytes
      case None => defaultValue
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
  def get(znode: String, defaultValue: Int): Int = {
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
  def get(znode: String, defaultValue: Float): Float = {
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
  def get(znode: String, defaultValue: Long): Long = {
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
  def get(znode: String, defaultValue: Double): Double = {
    getBytes(znode) match {
      case Some(bytes) => toDouble(bytes)
      case None => defaultValue
    }
  }

  /**
   * Get the value with the given znode path. Znode path will be created if
   * corresponded znode is not found with initial value.
   * @param znode denotes the destination path at which the value is stored.
   * @param initialValue if no value found.
   * @return String the value found at the znode path.
   */
  def getOrElse(znode: String, initialValue: String): String = 
    ifMatch[String]({ (client) => client.checkExists.forPath(znode) match {
      case stat: Stat => get(znode, initialValue)
      case _ => {
        create(znode) 
        set(znode, toBytes(initialValue))
        initialValue
      }
    }})

  /**
   * Get the value with the given znode path. Znode path will be created if
   * corresponded znode is not found with initial value.
   * @param znode denotes the destination path at which the value is stored.
   * @param initialValue if no value found.
   * @return Int is the value found at the znode path.
   */
  def getOrElse(znode: String, initialValue: Int): Int = 
    ifMatch[Int]({ (client) => client.checkExists.forPath(znode) match {
      case stat: Stat => get(znode, initialValue)
      case _ => {
        create(znode) 
        set(znode, toBytes(initialValue))
        initialValue
      }
    }})

  /**
   * Get the value with the given znode path. Znode path will be created if
   * corresponded znode is not found with initial value.
   * @param znode denotes the destination path at which the value is stored.
   * @param initialValue if no value found.
   * @return Float is the value found at the znode path.
   */
  def getOrElse(znode: String, initialValue: Float): Float = 
    ifMatch[Float]({ (client) => client.checkExists.forPath(znode) match {
      case stat: Stat => get(znode, initialValue)
      case _ => {
        create(znode) 
        set(znode, toBytes(initialValue))
        initialValue
      }
    }})

  /**
   * Get the value with the given znode path. Znode path will be created if
   * corresponded znode is not found with initial value.
   * @param znode denotes the destination path at which the value is stored.
   * @param initialValue if no value found.
   * @return Long is the value found at the znode path.
   */
  def getOrElse(znode: String, initialValue: Long): Long = 
    ifMatch[Long]({ (client) => client.checkExists.forPath(znode) match {
      case stat: Stat => get(znode, initialValue)
      case _ => {
        create(znode) 
        set(znode, toBytes(initialValue))
        initialValue
      }
    }})

  /**
   * Get the value with the given znode path. Znode path will be created if
   * corresponded znode is not found with initial value.
   * @param znode denotes the destination path at which the value is stored.
   * @param initialValue if no value found.
   * @return Double is the value found at the znode path.
   */
  def getOrElse(znode: String, initialValue: Double): Double = 
    ifMatch[Double]({ (client) => client.checkExists.forPath(znode) match {
      case stat: Stat => get(znode, initialValue)
      case _ => {
        create(znode) 
        set(znode, toBytes(initialValue))
        initialValue
      }
    }})
}
