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
package org.apache.hama.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import junit.framework.TestCase;
import org.apache.hadoop.fs.Path;
import org.apache.hama.bsp.FileSplit;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TestPartitionedSplit extends TestCase {

  public static final Log LOG = LogFactory.getLog(TestPartitionedSplit.class);
 
  static final Path partitionDir = new Path("/tmp/hama/io/");
  static final int partitionId = 9;
  static final int peerIndex = 7;
  static final String[] hosts = new String[]{ "host4", "host2", "host9" };
  static final long length = 64*1024*1024;

  PartitionedSplit createSplit() throws Exception {
    return new PartitionedSplit(FileSplit.class, partitionId, hosts, length);
  }

  public void testSerialization() throws Exception {
    final PartitionedSplit split = createSplit(); 
    final ByteArrayOutputStream bout = new ByteArrayOutputStream();
    final DataOutputStream out = new DataOutputStream(bout);
    split.write(out); 
    final byte[] bytes = bout.toByteArray();
    bout.close();
    final ByteArrayInputStream bin = new ByteArrayInputStream(bytes);
    final DataInputStream in = new DataInputStream(bin);
    final PartitionedSplit forVerification = new PartitionedSplit();
    forVerification.readFields(in);
    in.close();
    final String className = forVerification.splitClassName();
    assertEquals("Split class name should be "+FileSplit.class.getName(), 
                 FileSplit.class.getName(), className);
    final int pId = forVerification.partitionId();
    assertEquals("PartitionId should be "+partitionId, partitionId, pId);
    final String[] locations = forVerification.hosts();
    int idx = 0;
    for(String location: locations) {
      assertEquals("Split data should be stored at "+hostsString(), 
                   hosts[idx], location);
      idx++;
    }
    final long len = forVerification.length();
    assertEquals("Split length should be "+length, length, len);
  }

  public String hostsString() {
    final StringBuilder builder = new StringBuilder();
    for(String host: hosts) {
      builder.append(host+" "); 
    }
    return builder.toString();
  }

  public void testMerge() throws Exception {
  }
  
}
