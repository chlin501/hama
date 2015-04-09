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
package org.apache.hama.zk

import org.apache.curator.test.TestingServer
import org.apache.hama.logging.CommonLog
import scala.util.Failure
import scala.util.Success 
import scala.util.Try

object LocalZooKeeper {

  val zkPort = 2181

}

trait LocalZooKeeper extends CommonLog {

  import LocalZooKeeper._

  protected var server: Option[TestingServer] = None
  
  def launchZk {
    server = Option(new TestingServer(zkPort))
    LOG.info("Curator TestingServer is launched at port {}.", zkPort)
  }

  def launchZk(port: Int) = (0 < port && 65535 >= port) match {
    case true => {
      server = Option(new TestingServer(port))
      LOG.info("Curator TestingServer is launched at port {}.",port)
    }
    case false => throw new IllegalArgumentException(
      "Invalid ZooKeeper port value "+port)
  }

  def closeZk = server match {
    case None => LOG.warning("ZooKeeper instance not exist!")
    case Some(srv) => Try(srv.close) match {
      case Success(ok) =>
      case Failure(cause) => throw cause
    }
  }

}
