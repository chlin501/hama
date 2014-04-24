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
package org.apache.hama.master

/*

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.hama.LocalService
import org.apache.hama.HamaConfiguration
import org.apache.hama.bsp.v2.Job
import org.apache.hama.util.Curator
import org.apache.hama.util.Conversion
import org.apache.zookeeper.data.Stat
import scala.collection.immutable.Queue

class RuntimeInformation(conf: HamaConfiguration) extends LocalService 
                                                  with Curator 
                                                  with Conversion {
 
  val masterPath = "/bsp/masters/" + masterName + "/id"

  val seqPath = "/bsp/masters/" + masterName + "/seq"

  val totalTaskCapacityPath = "/bsp/masters/" + masterName + "/tasks/capacity"

  def masterName: String = conf.get("bsp.master.name", "bspmaster")

  override def log(msg: String) = LOG.info(msg)

  override def configuration: HamaConfiguration = conf

  override def name: String = "runtimeInformation"

  override def initializeServices = initializeCurator(configuration) 

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

   * Recursive create ZooKeeper directory.
   * @param path
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

  private def createZnode(path: String) = {
    if(path.startsWith("/")) {
      val nodes = path.split("/").drop(1)
      var p = "" 
      nodes.foreach( node => {
        p += "/"+node
        LOG.info("Path to be checked then created -> {} ", p)
        curatorFramework.checkExists.forPath(p) match {
          case stat: Stat => 
          case _ => curatorFramework.create.forPath(p) 
        }
      })
    } else throw new IllegalArgumentException("Path "+path+" is not started "+
                                              "from root '/'")
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
*/
