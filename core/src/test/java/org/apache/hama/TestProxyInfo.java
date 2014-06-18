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
package org.apache.hama;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TestProxyInfo extends TestCase {

  final Log LOG = LogFactory.getLog(TestProxyInfo.class);

  public void testBuilder() throws Exception {
    final HamaConfiguration conf = new HamaConfiguration();
    final ProxyInfo info = new ProxyInfo.MasterBuilder("testActor", conf).
                                         createActorPath().
                                         appendRootPath("bspmaster").
                                         appendChildPath("monitor").
                                         appendChildPath("testActor").
                                         build();
    assertNotNull("Proxy shouldn't be null.", info);

    final String actorSystemName = info.getActorSystemName();
    LOG.info("Actor system name is "+actorSystemName);
    assertEquals("Actor system name should be "+actorSystemName, 
                 actorSystemName, "MasterSystem");

    final String host = info.getHost();
    LOG.info("Host value is "+host);
    assertEquals("Host value should be "+host, host, "127.0.0.1");

    final int port = info.getPort();
    LOG.info("Port value is "+port);
    assertEquals("Port value should be "+port, port, 40000);

    final String actorFullPath = info.getPath();
    LOG.info("Actor full path is "+actorFullPath);
    assertEquals("Full actor path is "+actorFullPath, actorFullPath, 
                 "akka.tcp://MasterSystem@127.0.0.1:40000/user/bspmaster/" +
                 "monitor/testActor");
  }
}
