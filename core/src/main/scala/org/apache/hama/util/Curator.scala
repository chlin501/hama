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

import akka.routing._
import org.apache.curator.framework._
import org.apache.curator.retry._
import org.apache.hama._
import org.apache.hama.bsp.v2.Job
import org.apache.hama.master._
import org.apache.hama.zookeeper._
import org.apache.zookeeper.data._
import scala.collection.immutable.Queue

class Curator(conf: HamaConfiguration) extends LocalService {

  private var curatorFramework: CuratorFramework = _

  override def configuration: HamaConfiguration = conf

  override def name: String = "curator"

  def createCurator(servers: String, timeout: Int, n: Int, 
                    delay: Int): CuratorFramework = {
    CuratorFrameworkFactory.builder().
                            connectionTimeoutMs(timeout).
                            retryPolicy(new RetryNTimes(n, delay)).
                            connectString(servers).build
  }

  override def initializeServices {
    val connectString = QuorumPeer.getZKQuorumServersString(conf)
    val sessionTimeout = conf.getInt(Constants.ZOOKEEPER_SESSION_TIMEOUT, 
                                     3*60*1000)
    val retriesN = conf.getInt("bsp.zookeeper.client.retry_n_times", 10)
    val sleepBetweenRetries = 
      conf.getInt("bsp.zookeeper.client.sleep_between_delay", 1000)
    curatorFramework = createCurator(connectString, sessionTimeout, 
                                     retriesN, sleepBetweenRetries)
  }

  private def getMasterId(fullPath: String): Option[String] = {
    curatorFramework.checkExists.forPath(fullPath) match {
      case stat: Stat => {
        curatorFramework.getData().forPath(fullPath) match {
          case value: Array[Byte] => Some(new String(value))
          case _ =>  None 
        }
      }
      case _ => {
        LOG.warning("Not found master id at path {}.", fullPath)
        None
      }
    }
  }

  override def receive = {
    isServiceReady orElse serverIsUp orElse
    //case Get(path) => {
      //sender ! getMasterId(path)
    //}
    //case Create(path, value) => {
      // write to zk
    //}
    /*({case GetNewJobId(job: Job) => {
      LOG.info("Received job {} submitted from the client.",job.getName) 
    }}: Receive) orElse*/ unknown
  } 
}
