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

public class TestSystemInfo extends TestCase {

  final Log LOG = LogFactory.getLog(TestSystemInfo.class);

  public void testEquals() throws Exception {
    final SystemInfo info1 = new SystemInfo("sys1", "host1", 1234);
    final SystemInfo info2 = new SystemInfo("sys1", "host1", 1234);
    assertEquals("Remote node should be the same.", info1, info2);

    final SystemInfo info3 = new SystemInfo("localSys");
    final SystemInfo info4 = new SystemInfo("localSys");
    assertEquals("Local node should be the same.", info3, info4);
  }

}
