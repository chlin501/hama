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
import org.apache.curator.framework.api.Pathable
import org.apache.curator.framework.imps.CuratorFrameworkState._
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier
import org.apache.curator.retry.RetryNTimes
import org.apache.zookeeper.CreateMode
import org.apache.zookeeper.data.Stat
import scala.collection.JavaConversions._

object Curator extends CommonLog {

  val threeMinutes: Int = 3*60*1000

  def build(servers: String, sessionTimeout: Int, retryN: Int,
            delay: Int): CuratorFramework =
    CuratorFrameworkFactory.builder.sessionTimeoutMs(sessionTimeout).
                                    retryPolicy(new RetryNTimes(retryN, delay)).
                                    connectString(servers).build

  def build(conf: HamaConfiguration): CuratorFramework = {
    val servers = conf.get("hama.zookeeper.property.connectString", 
                           "localhost:2181")
    val sessionTimeout = conf.getInt("hama.zookeeper.session.timeout",
                                     threeMinutes)
    val retryN = conf.getInt("bsp.zookeeper.client.retry_n_times", 10)
    val sleepBeforeRetry =
      conf.getInt("bsp.zookeeper.client.sleep_before_retry", 1000)
    LOG.info("Properties for ZooKeeper connection contain servers: {},"+
             "sessionTimeout: {}, retriesN: {}, sleepBeforeRetry: {}.",
             servers, sessionTimeout, retryN, sleepBeforeRetry)
    build(servers, sessionTimeout, retryN, sleepBeforeRetry) 
  }

  def start(client: CuratorFramework) = client.getState match {
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

trait Curator extends Conversion with CommonLog {

  import Curator._

  // TODO: create instance from factory method. and singleton.
  protected var curatorFramework: Option[CuratorFramework] = None 

  /**
   * Initialize curator instance.
   * @param conf contains information for connecting to ZooKeeper, including
   *             connectString, sessionTimeout, retriesN, and 
   *             sleepBetweenRetries.
   */
  def initializeCurator(conf: HamaConfiguration): Option[CuratorFramework] = 
    curatorFramework match { 
      case Some(client) => { 
        start(client) 
        curatorFramework
      }
      case None => {
        val client = build(conf)
        curatorFramework = Option(client)
        start(client)
        curatorFramework
      }
    }

  /**
   * Operate on a function if {@link CuratorFramework} exists.
   * @param f is a function that takes in curator framework instance and 
   *          return a value.
   * @throw if curator framework is missing.
   */
  protected def exist[R <: Any](f: (CuratorFramework) => R): R = 
    curatorFramework match {
      case Some(client) => f(client)
      case None => throw new RuntimeException("Curator not initialized!")
    }

  protected def exist(znode: String): Boolean = curatorFramework match {
    case Some(client) => client.checkExists.forPath(znode) match {
      case _: Stat => true
      case _ => false
    }
    case None => throw new RuntimeException("Curator not initialized!") 
  }

  /**
   * Recursively create znode without value being set.
   * The znode must be an absolute path.
   * Note this is not an atomic operation. 
   * Use {@link org.apache.curator.framework.api.transaction.CuratorTransaction}
   * instead if needed.
   * @param znode denotes the path to be created in ZooKeeper.
   */
  def create(znode: String): Unit = create(znode, CreateMode.PERSISTENT)

 
  /**
   * Recursively create znode. This function doesn't set znode with value, and 
   * CreateMode needs to be specified.
   * The znode must be an absolute path.
   * Note this is not an atomic operation. 
   * @param znode denotes the path to be created in ZooKeeper.
   * @param mode denotes created znode type according to ZooKeeper CreateMode.
   */  
  def create(znode: String, mode: CreateMode): Unit = mode match {
    case null => throw new IllegalArgumentException("Create mode is missing!")
    case m@_ => {
      if(null == znode || znode.isEmpty || !znode.startsWith("/"))
        throw new IllegalArgumentException("Znode is not started from '/', " +
                                           "empty or null value: "+znode)
      exist({ client => client.create.creatingParentsIfNeeded.withMode(m).
                               forPath(znode) })
    }
  }

  /**
   * Create znode children based on parent znode and children znode array.
   * @param parent points the parent znode. 
   * @param children is an array containing all children znode.
   */
  def create(parent: String, children: Array[String]): Unit = 
    create(parent, children, None) 

  /**
   * Create children znodes with values supplided.
   * @param parent points to the parent znode.
   * @param children is an array containing all children znode.
   * @param values is an array containing all children values.
   */
  def create(parent: String, children: Array[String], values: Array[Any]): 
    Unit = create(parent, children, Option(values))

  /**
   * Create children znodes with values supplided.
   * @param parent points to the parent znode.
   * @param children is an array containing all children znode.
   * @param values is an array option containing all children values.
   * @param mode denotes created znode type according to ZooKeeper CreateMode. 
   */
  def create(parent: String, 
             children: Array[String], 
             values: Option[Array[Any]], 
             mode: CreateMode = CreateMode.PERSISTENT): Unit = 
    children.map { child => {
      val znodeToChild = "%s/%s".format(parent, child)
      create(znodeToChild, mode)
      znodeToChild
    }}.zipWithIndex.foreach( e => values.map { value => value(e._2) match {
      case i: Int => set(e._1, toBytes(i))
      case f: Float => set(e._1, toBytes(f))
      case l: Long => set(e._1, toBytes(l))
      case d: Double => set(e._1, toBytes(d))
      case s: String => set(e._1, toBytes(s))
      case rest@_  => throw new RuntimeException("Not yet supported: "+rest)
    }}) 
 
  /**
   * List all children under a particular znode.
   * @param znode points to the parent path where children znodes exist. 
   * @return Array of String type; at least an empty String array.
   */
  def list(parent: String): Array[String] = exist({ (client) => {
    client.checkExists.forPath(parent) match {
      case stat: Stat => client.getChildren.forPath(parent).map { _.toString }.
                                toArray
      case _ => Array[String]()
    }
  }})

  /**
   * Set data at znode with corresponded value.
   * @param znode the absolute path where data to be set.
   * @param value is the data to be set. 
   */
  def set(znode: String, value: Array[Byte]) = exist({ (client) => 
    client.setData.forPath(znode, value) 
  })

  /**
   * Get bytes from the designated znode, but value won't be set at ZooKeeper.
   * @param znode is the destination path where data is stored.
   * @return Option[Array[Byte]] is the data retrieved out of znode.
   */
  def getBytes(znode: String): Option[Array[Byte]] = 
    exist[Option[Array[Byte]]]({ (client) =>
      client.getData.forPath(znode) match {
        case data: Array[Byte] => Option(data)
        case _ => None
      }
    })

  /**
   * Get bytes with default value, but value won't be set at ZooKeeper.
   * @param znode denotes the absolute path in ZooKeeper.
   * @param defaultValue is applied if not found.
   * @return Array[Byte] if found at znode; otherwise returns defaultValue.
   */
  def getBytes(znode: String, defaultValue: Array[Byte]): 
    Array[Byte] = getBytes(znode) match {
      case Some(bytes) => bytes
      case None => defaultValue
    }
  
  /**
   * Get string with default value, but value won't be set at ZooKeeper.
   * @param znode denotes the absolute path in ZooKeeper.
   * @param defaultValue is applied if not found.
   * @return String if found at znode; otherwise returns defaultValue.
   */
  def get(znode: String, defaultValue: String): String = 
    getBytes(znode) match {
      case Some(bytes) => new String(bytes)
      case None => defaultValue 
    }

  /**
   * Get int with default value, but value won't be set at ZooKeeper.
   * @param znode denotes the absolute path in ZooKeeper.
   * @param defaultValue is applied if not found.
   * @return Int if found at znode; otherwise returns defaultValue.
   */
  def get(znode: String, defaultValue: Int): Int = getBytes(znode) match {
    case Some(bytes) => toInt(bytes)
    case None => defaultValue
  }

  /**
   * Get float with default value, but value won't be set at ZooKeeper.
   * @param znode denotes the absolute path in ZooKeeper.
   * @param defaultValue is applied if not found.
   * @return Float if found at znode; otherwise returns defaultValue.
   */
  def get(znode: String, defaultValue: Float): Float = getBytes(znode) match {
    case Some(bytes) => toFloat(bytes)
    case None => defaultValue
  }

  /**
   * Get long with default value, but value won't be set at ZooKeeper.
   * @param znode denotes the absolute path in ZooKeeper.
   * @param defaultValue is applied if not found.
   * @return Long if found at znode; otherwise returns defaultValue.
   */
  def get(znode: String, defaultValue: Long): Long = getBytes(znode) match {
    case Some(bytes) => toLong(bytes)
    case None => defaultValue
  }

  /**
   * Get double with default value, but value won't be set at ZooKeeper.
   * @param znode denotes the absolute path in ZooKeeper.
   * @param defaultValue is applied if not found.
   * @return Double if found at znode; otherwise returns defaultValue.
   */
  def get(znode: String, defaultValue: Double): Double = getBytes(znode) match {
    case Some(bytes) => toDouble(bytes)
    case None => defaultValue
  }

  /**
   * Get the value with the given znode path, that will be created if
   * corresponded znode is not found with initial value.
   * @param znode denotes the destination path at which the value is stored.
   * @param initialValue if no value found.
   * @return String the value found at the znode path.
   */
  def getOrElse(znode: String, initialValue: String): String = 
    exist[String]({ (client) => client.checkExists.forPath(znode) match {
      case stat: Stat => get(znode, initialValue)
      case _ => {
        create(znode) 
        set(znode, toBytes(initialValue))
        initialValue
      }
    }})

  /**
   * Get the value with the given znode path, that will be created if
   * corresponded znode is not found with initial value.
   * @param znode denotes the destination path at which the value is stored.
   * @param initialValue if no value found.
   * @return Int is the value found at the znode path.
   */
  def getOrElse(znode: String, initialValue: Int): Int = 
    exist[Int]({ (client) => client.checkExists.forPath(znode) match {
      case stat: Stat => get(znode, initialValue)
      case _ => {
        create(znode) 
        set(znode, toBytes(initialValue))
        initialValue
      }
    }})

  /**
   * Get the value with the given znode path, that path will be created if
   * corresponded znode is not found with initial value.
   * @param znode denotes the destination path at which the value is stored.
   * @param initialValue if no value found.
   * @return Float is the value found at the znode path.
   */
  def getOrElse(znode: String, initialValue: Float): Float = 
    exist[Float]({ (client) => client.checkExists.forPath(znode) match {
      case stat: Stat => get(znode, initialValue)
      case _ => {
        create(znode) 
        set(znode, toBytes(initialValue))
        initialValue
      }
    }})

  /**
   * Get the value with the given znode path, that will be created if
   * corresponded znode is not found with initial value.
   * @param znode denotes the destination path at which the value is stored.
   * @param initialValue if no value found.
   * @return Long is the value found at the znode path.
   */
  def getOrElse(znode: String, initialValue: Long): Long = 
    exist[Long]({ (client) => client.checkExists.forPath(znode) match {
      case stat: Stat => get(znode, initialValue)
      case _ => {
        create(znode) 
        set(znode, toBytes(initialValue))
        initialValue
      }
    }})

  /**
   * Get the value with the given znode path, that will be created if
   * corresponded znode is not found with initial value.
   * @param znode denotes the destination path at which the value is stored.
   * @param initialValue if no value found.
   * @return Double is the value found at the znode path.
   */
  def getOrElse(znode: String, initialValue: Double): Double = 
    exist[Double]({ (client) => client.checkExists.forPath(znode) match {
      case stat: Stat => get(znode, initialValue)
      case _ => {
        create(znode) 
        set(znode, toBytes(initialValue))
        initialValue
      }
    }})

  /**
   * Close curator instance.
   */
  def closeCurator() = curatorFramework.map { (client) => client.close }
}
