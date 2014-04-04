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

import akka.routing._
import java.nio._
import java.text._
import java.util._
import org.apache.curator.framework._
import org.apache.curator.retry._
import org.apache.hama._
import org.apache.hama.bsp.v2.Job
import org.apache.hama.master._
import org.apache.hama.zookeeper._
import org.apache.zookeeper.data._
import scala.collection.immutable.Queue
import scala.collection.JavaConversions._

class Curator(conf: HamaConfiguration) extends LocalService {

  private var curatorFramework: CuratorFramework = _
 
  val masterPath = 
    "/bsp/masters/" + conf.get("bsp.master.name", "bspmaster") + "/id"

  val seqPath = 
    "/bsp/masters/" + conf.get("bsp.master.name", "bspmaster") + "/seq"

  val totalTaskCapacityPath = 
    "/bsp/masters/" + conf.get("bsp.master.name", "bspmaster") + "/tasks" +
    "/capacity"

  override def configuration: HamaConfiguration = conf

  override def name: String = "curator"

  private def createCurator(servers: String, timeout: Int, n: Int, 
                    delay: Int): CuratorFramework = {
    CuratorFrameworkFactory.builder().
                            connectionTimeoutMs(timeout).
                            retryPolicy(new RetryNTimes(n, delay)).
                            connectString(servers).build
  }

  override def initializeServices {
    val connectString = 
      conf.get("hama.zookeeper.property.connectString", "localhost:2181") 
    val sessionTimeout = conf.getInt(Constants.ZOOKEEPER_SESSION_TIMEOUT, 
                                     3*60*1000)
    val retriesN = conf.getInt("bsp.zookeeper.client.retry_n_times", 10)
    val sleepBetweenRetries = 
      conf.getInt("bsp.zookeeper.client.sleep_between_delay", 1000)
    LOG.info("Properties for ZooKeeper connection -> connectString: {},"+
             "sessionTimeout: {}, retriesN: {}, sleepBetweenRetires: {}.", 
             connectString, sessionTimeout, retriesN, sleepBetweenRetries)
    curatorFramework = createCurator(connectString, sessionTimeout, 
                                     retriesN, sleepBetweenRetries)
    curatorFramework.start
    LOG.info("CuratorFramework is started!")
  }

  private def nextSequence: Option[Int] = {
    curatorFramework.checkExists.forPath(seqPath) match {
      case stat: Stat => {
        val seq = getValue[Int](seqPath, classOf[Int]).getOrElse(-1)
        if(-1 == seq) None else Some((seq+1))
      }
      case _ => {
        createPath(masterPath, createZnode)
        LOG.info("Not found job seq at {}, creating a new one {}.", 
                 seqPath, 1)
        createValue(seqPath, toBytes(1))
        Some(1)
      }
    }
  }

  private def toInt(data: Array[Byte]): Int = ByteBuffer.wrap(data).getInt
 
  private def toFloat(data: Array[Byte]): Float = 
    ByteBuffer.wrap(data).getFloat

  private def toLong(data: Array[Byte]): Double = 
    ByteBuffer.wrap(data).getLong

  private def toDouble(data: Array[Byte]): Double = 
    ByteBuffer.wrap(data).getDouble

  private def toBytes(data: Int): Array[Byte] = 
    ByteBuffer.allocate(4).putInt(data).array()

  private def toBytes(data: Float): Array[Byte] = 
    ByteBuffer.allocate(4).putFloat(data).array()

  private def toBytes(data: Long): Array[Byte] = 
    ByteBuffer.allocate(8).putLong(data).array()

  private def toBytes(data: Double): Array[Byte] = 
    ByteBuffer.allocate(8).putDouble(data).array()

  private def createMasterId: String =
    new SimpleDateFormat("yyyyMMddHHmm").format(new Date())

  private def findMasterId: Option[String] = {
    curatorFramework.checkExists.forPath(masterPath) match {
      case stat: Stat => getValue[String](masterPath, classOf[String])
      case _ => {
        createPath(masterPath, createZnode)
        val masterId = createMasterId
        LOG.info("Not found master id at {}, creating a new one {}.", 
                 masterPath, masterId)
        createValue(masterPath, masterId.getBytes)
        Some(masterId)
      }
    }
  }

  // TODO: use manifest?
  private def getValue[T](fullPath: String, cls: Class[_]): Option[T] = {
    val ClassOfString = classOf[String]; val ClassOfInt = classOf[Int]
    val ClassOfFloat = classOf[Float]; val ClassOfLong = classOf[Long]
    val ClassOfDouble = classOf[Double]
    curatorFramework.getData.forPath(fullPath) match {
      case data: Array[Byte] => {
        val value = cls match  {
          case ClassOfString => new String(data)
          case ClassOfInt => toInt(data)
          case ClassOfFloat => toFloat(data)
          case ClassOfLong => toLong(data)
          case ClassOfDouble => toDouble(data)
          case _ => None 
        }
        LOG.info("Value {} found at {}.", value, fullPath)
        Some(value.asInstanceOf[T]) 
      }
      case _ => None 
    }
  }

  private def createValue(fullPath: String, data: Array[Byte]) {
    curatorFramework.setData.forPath(fullPath, data)
  }

  /**
   * Recursive create ZooKeeper directory.
   * @param path
   */
  private def createPath(path: String, c: (String) => Unit) {
    val nodes = path.split("/").drop(1) // drop the first empty string
    var result = "/"
    var depth = 0
    nodes.foreach(node => {
      result += node
      c(result)
      if(depth != (nodes.size-1)) result += "/"
      depth += 1
    })
  }

  TODO: bug! Node exists. Need to check if path exists first e.g. /bsp
  private def createZnode(path: String) = {
    curatorFramework.create.forPath(path) 
  }

  def getMasterId: Receive = {
    case GetMasterId => {
      sender ! MasterId(findMasterId)
    }
  }

  def getJobSeq: Receive = {
    case GetJobSeq => {
      sender ! JobSeq(nextSequence.getOrElse(-1))
    }
  }

  def totalTaskCapacity: Receive = {
    case TotalTaskCapacity(maxTasks) => updateTotalTaskCapacity(maxTasks) 
  }
 
  private def updateTotalTaskCapacity(maxTasks: Int)  = {
    curatorFramework.checkExists.forPath(totalTaskCapacityPath) match {
      case stat: Stat => {
        val v = getValue[Int](totalTaskCapacityPath, 
                              classOf[Int]).getOrElse(-1)
        if(-1 == v) 
          throw new RuntimeException("TotalTaskCapacity at "+
                                     totalTaskCapacityPath+" shouldn't be -1.") 
        createValue(totalTaskCapacityPath, toBytes(maxTasks+v))
      }
      case _ => {
        createPath(totalTaskCapacityPath, createZnode)
        LOG.info("Not found totalTaskCapacity at {}, creating a new one {}.", 
                 totalTaskCapacityPath, 0)
        createValue(totalTaskCapacityPath, toBytes(maxTasks))
      }
    }
  }

  override def receive = isServiceReady orElse serverIsUp orElse getMasterId orElse getJobSeq orElse totalTaskCapacity orElse unknown

}
