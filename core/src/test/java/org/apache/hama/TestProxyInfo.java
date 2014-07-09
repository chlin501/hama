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

  ProxyInfo createMasterProxy(final HamaConfiguration conf) throws Exception {
    final ProxyInfo info = new ProxyInfo.MasterBuilder("testActor", conf).
                                         createActorPath().
                                         appendRootPath("bspmaster").
                                         appendChildPath("monitor").
                                         appendChildPath("testActor").
                                         build();
    return info;
  }

  ProxyInfo createGroomProxy(final HamaConfiguration conf) throws Exception {
    final ProxyInfo info = new ProxyInfo.GroomBuilder("testActor1", conf).
                                         createActorPath().
                                         appendRootPath("groomServer").
                                         appendChildPath("monitor").
                                         appendChildPath("testActor1").
                                         build();
    return info;
  }  

  public void testMasterBuilder() throws Exception {
    final HamaConfiguration conf = new HamaConfiguration();
    final ProxyInfo info = createMasterProxy(conf);
    assertNotNull("Master proxy shouldn't be null.", info);

    final String actorSystemName = info.getActorSystemName();
    LOG.info("Master actor system name is "+actorSystemName);
    assertEquals("Master actor system name should be "+actorSystemName, 
                 actorSystemName, "MasterSystem");

    final String host = info.getHost();
    LOG.info("Master host value is "+host);
    assertEquals("Master host value should be "+host, host, "127.0.0.1");

    final int port = info.getPort();
    LOG.info("Master port value is "+port);
    assertEquals("Master port value should be "+port, port, 40000);

    final String actorFullPath = info.getPath();
    LOG.info("Master actor full path is "+actorFullPath);
    assertEquals("Full actor path at master is "+actorFullPath, actorFullPath, 
                 "akka.tcp://MasterSystem@127.0.0.1:40000/user/bspmaster/" +
                 "monitor/testActor");
  }

  public void testGroomBuilder() throws Exception {
    final HamaConfiguration conf = new HamaConfiguration();
    final ProxyInfo info = createGroomProxy(conf);
    assertNotNull("Groom proxy shouldn't be null.", info);

    final String actorSystemName = info.getActorSystemName();
    LOG.info("Groom actor system name is "+actorSystemName);
    assertEquals("Groom actor system name should be "+actorSystemName, 
                 actorSystemName, "GroomSystem");

    final String host = info.getHost();
    LOG.info("Groom host value is "+host);
    assertEquals("Groom host value should be "+host, host, "127.0.0.1");

    final int port = info.getPort();
    LOG.info("Groom port value is "+port);
    assertEquals("Groom port value should be "+port, port, 50000);

    final String actorFullPath = info.getPath();
    LOG.info("Groom actor full path is "+actorFullPath);
    assertEquals("Full actor path at Groom is "+actorFullPath, actorFullPath, 
                 "akka.tcp://GroomSystem@127.0.0.1:50000/user/groomServer/" +
                 "monitor/testActor1");
  }

  public void testFromString() throws Exception {
    final String remote = 
      "akka.tcp://bspPeerSystem1@host14:1946/user/peerMessenger";
    final String local = "akka://bspPeerSystem1/user/peerMessenger";
    final ProxyInfo remoteProxy = ProxyInfo.fromString(remote);
    assertNotNull("Remote proxy not null!", remoteProxy);
    assertEquals("Remote proxy path reconstructed should equal.", 
                 remote, remoteProxy.getPath());
    assertEquals("Remote actor name should be peerMessenger", 
                 "peerMessenger", remoteProxy.getActorName());
    final ProxyInfo localProxy = ProxyInfo.fromString(local);
    assertNotNull("Local proxy not null!", localProxy);
    assertEquals("Local path reconstructed should equal.", 
                 local, localProxy.getPath());
    assertEquals("Local actor name should be peerMessenger", 
                 "peerMessenger", localProxy.getActorName());
  }
}
