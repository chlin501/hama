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
import org.apache.hama.logging.Logger

trait LocalZooKeeper extends Logger {

  protected var server: TestingServer = _
  
  def launchZk {
    server = new TestingServer(2181)
    LOG.info("Curator TestingServer is launched at port 2181.")
  }

  def launchZk(port: Int) {
    server = new TestingServer(port)
    LOG.info("Curator TestingServer is launched at port %s.".format(port))
  }

  def closeZk = if(null != server) server.close

}
