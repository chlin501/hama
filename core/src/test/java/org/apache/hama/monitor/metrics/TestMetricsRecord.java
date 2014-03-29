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
package org.apache.hama.monitor.metrics;

import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.io.DataInputStream;
import java.util.List;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ObjectWritable;
import org.apache.hadoop.io.Writable;

/**
 * Test (De)Serialize functions.
 */
public class TestMetricsRecord extends TestCase {

  final Log LOG = LogFactory.getLog(TestMetricsRecord.class);

  MetricsRecord createRecord() throws Exception {
    final MetricsRecord record = 
      new MetricsRecord("groom_0.0.0.0_50000", "jvm", "jvm metrics stat.");

    final Writable mem = new LongWritable(1024);
    record.add(new Metric("mem", "mem desc.", LongWritable.class, mem));

    final Writable gc = new IntWritable(30);
    record.add(new Metric("gc", "gc desc.", IntWritable.class, gc));

    final Writable threads = new LongWritable(40);
    record.add(new Metric("threads", "threads desc.", LongWritable.class, 
                          threads));
    return record;
  } 

  byte[] serialize(Writable writable) throws Exception {
    final ByteArrayOutputStream bout = new ByteArrayOutputStream();
    final DataOutputStream out = new DataOutputStream(bout);
    try { 
      writable.write(out);
    } finally { 
      out.close(); 
    }
    return bout.toByteArray();
  }

  MetricsRecord deserialize(byte[] bytes) throws Exception {
    final ByteArrayInputStream bin = new ByteArrayInputStream(bytes);
    final DataInputStream in = new DataInputStream(bin);
    final MetricsRecord record = new MetricsRecord();
    try {
      record.readFields(in);
    } finally {
      in.close();
    } 
    return record;
  }

  public void testSerialization() throws Exception {
    final MetricsRecord record = createRecord();
    final byte[] bytes = serialize(record);
    final MetricsRecord forVerification = deserialize(bytes);
    LOG.info("Restored MetricsRecord is "+forVerification);
    
    LOG.info("MetricsRecord groomName is "+forVerification.getGroomName());
    assertEquals("MetricsRecord GroomName should be the same.", 
                 record.getGroomName(), forVerification.getGroomName());

    LOG.info("MetricsRecord name is "+forVerification.getName());
    assertEquals("MetricsRecord Name should be the same.", 
                 record.getName(), forVerification.getName());

    LOG.info("MetricsRecord description is "+forVerification.getDescription());
    assertEquals("MetricsRecord Description should be the same.", 
                 record.getDescription(), forVerification.getDescription());


    List<Metric> metrics = forVerification.getMetrics();
    assertNotNull("MetricsRecord metrics can't be null! ", metrics);
    assertTrue("MetricsRecord metrics size should be 3! ", 3 == metrics.size());
    LOG.info("MetricsRecord metrics is "+metrics);
    for(Metric metric: metrics) {
      final String name = metric.getName();
      final String desc = metric.getDescription();
      final Class<? extends Writable> desclared = metric.getDeclared();
      final Writable value = metric.getValue();
      LOG.info("Metric name: "+name+" description: "+desc+" value: "+value);
      assertTrue("Metric name should be mem, gc, or threads.", 
                 name.equals("mem") || name.equals("gc") || 
                 name.equals("threads"));
      if("mem".equals(name) || "threads".equals(name)) {
        assertTrue(((LongWritable)value).get() == 1024 ||
                   ((LongWritable)value).get() == 40);
      } else if("gc".equals(name)) {
        assertTrue(((IntWritable)value).get() == 30);
      }
    }
  }

}
