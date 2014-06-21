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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import junit.framework.TestCase;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hama.bsp.Counters;
import org.apache.hama.bsp.FileSplit;
import org.apache.hama.bsp.InputSplit;
import org.apache.hama.bsp.OutputCollector;
import org.apache.hama.bsp.RecordReader;
import org.apache.hama.fs.HDFSLocal;
import org.apache.hama.fs.Operation;
import org.apache.hama.fs.OperationFactory;
import org.apache.hama.HamaConfiguration;

public class TestDefaultIO extends TestCase {

  public static final Log LOG = LogFactory.getLog(TestDefaultIO.class);

  private static final String outputDir = "/tmp/hama-parts/job_test_0002/";
  private static final int[] peerIndexes = new int[]{ 4, 1, 7 };
  private static final int[] partitions = new int[]{ 1, 2, 3 };
  private static final String[] fileSplitLocation1 = new String[]{ "host1", 
                                                                   "host5", 
                                                                   "host3" };
  final HamaConfiguration conf = new HamaConfiguration();
  
  class MockDefaultIO extends DefaultIO {

/*
    MockDefaultIO(final HamaConfiguration conf,  
                  final PartitionedSplit split) {
      super(conf, split, new Counters());
    }

    MockDefaultIO(final PartitionedSplit split) {
      super(conf, split, new Counters());
    }
*/

    // writer
    @Override 
    public Path outputPath(final long timestamp, final int partitionId) {
      final Path out = super.outputPath(timestamp, partitionId);
      final Path expected = new Path("/tmp/hama/io/defaultio/", 
                                     childPath(partitionId));
      assertEquals("Ouptut path should be "+expected.toString(), 
                   expected.toString(), out.toString()); 
      return out;
    }

    // reader
    @Override
    public RecordReader createRecordReader(final InputSplit split) 
        throws IOException {
      assertNotNull("InputSplit supplied can't be null!", split);
      final FileSplit fileSplit = (FileSplit) split;
      assertEquals("Split class name should be "+FileSplit.class.getName(), 
                   super.split.splitClassName(), 
                   FileSplit.class.getName());
      assertEquals("File size to be processed should equal.", 
                   super.split.length(), fileSplit.getLength());
      assertEquals("Partition id should be "+partitions[0],
                   super.split.partitionId(), partitions[0]);
      final String[] hosts = super.split.hosts();
      assertNotNull("Split hosts can't be null!", hosts);
      LOG.info("PartitionedSplit's hosts length is "+ hosts.length);
      final String[] locations = fileSplit.getLocations();
      assertNotNull("File split locations can't be null!", locations);
      LOG.info("FileSplit's locations length is "+ locations.length);
      /* N.B.: FileSplit.write() doesn't serialize hosts fields! */
      assertEquals("File split's location length must be 0!", 
                   0, locations.length);
      for(int idx=0; idx < hosts.length; idx++) {
        LOG.info("host["+idx+"] value is "+hosts[idx]);
        assertEquals("Split host at "+idx+" should equal!", 
                     hosts[idx], fileSplitLocation1[idx]);
      }
      final ByteArrayOutputStream bout = new ByteArrayOutputStream();
      final DataOutputStream buffer = new DataOutputStream(bout);
      fileSplit.write(buffer);
      byte[] splitBytes = bout.toByteArray();
      int dataLength = splitBytes.length;
      LOG.info(fileSplit.getClass().getName()+" has length: "+dataLength); 
      LOG.info(super.split.getClass().getName()+" has "+ super.split.length()+
               " bytes.");
      assertEquals("Split length value should be 1024L.", 
                   1024L, super.split.length());
      byte[] partitionedBytes = super.split.bytes();
      LOG.info(super.split.getClass().getName()+" has "+partitionedBytes.length+
               " bytes.");
      assertEquals("Bytes length should equal!", 
                   partitionedBytes.length, splitBytes.length);
      for(int pos=0; pos < partitionedBytes.length; pos++) {
        assertEquals("Split's bit value should be equals!", 
                     splitBytes[pos], partitionedBytes[pos]);
      } 
      buffer.close(); 
      return null;
    }
  }

  List<PartitionedSplit> createSplits() throws Exception {
    final List<InputSplit> inputSplits = createMockFileSplits();
    final int splitSize = inputSplits.size();
    LOG.info("PartitionedSplit list size is "+splitSize);
    final List<PartitionedSplit> splits = new ArrayList<PartitionedSplit>();
    for(int idx = 0; idx < splitSize; idx++) {
      final InputSplit inputSplit = inputSplits.get(idx);
      final String splitClassName = inputSplit.getClass().getName();
      LOG.info("Split class name is "+splitClassName);
      final ByteArrayOutputStream bout = new ByteArrayOutputStream();
      final DataOutputStream buffer = new DataOutputStream(bout);
      inputSplit.write(buffer);
      byte[] data = bout.toByteArray(); 
      LOG.info(splitClassName +" has "+data.length+" bytes."); 
      final PartitionedSplit split = 
        new PartitionedSplit(FileSplit.class, partitions[idx],
                             inputSplit.getLocations(), inputSplit.getLength(),
                             data, 0, data.length);
      splits.add(split);
      buffer.close();
    }
    return splits;
  }
  
  List<InputSplit> createMockFileSplits() throws Exception {
    List<InputSplit> fileSplits = new ArrayList<InputSplit>();
    fileSplits.add(new FileSplit(new Path(outputDir+"part-"+partitions[0]+
                                          "/part-"+peerIndexes[0]), 
                                 0L, 1024L, 
                                 fileSplitLocation1));
/*
    fileSplits.add(new FileSplit(new Path(outputDir+"part-"+partitions[1]+
                                          "/part-"+peerIndexes[1]), 
                                 1025L, 4096L, 
                                 new String[]{ "host5", "host2", "host7" }));
    fileSplits.add(new FileSplit(new Path(outputDir+"part-"+partitions[2]+
                                          "/part-"+peerIndexes[2]), 
                                 5121L, 2048L, 
                                 new String[]{ "host10", "host11", "host2" }));
*/
    return fileSplits;
  }
 
  public void testReader() throws Exception {
    final List<PartitionedSplit> splits = createSplits();
    assertEquals("Split size should be 1!", 1, splits.size());
    //final MockDefaultIO io = new MockDefaultIO(splits.get(0));
    final MockDefaultIO io = new MockDefaultIO();
    io.setConf(conf);
    io.initialize(splits.get(0), new Counters());
    io.reader();
  }  

  public void testWriter() throws Exception {
    conf.set("bsp.output.dir", "/tmp/hama/io/defaultio/");
    conf.setClass("bsp.fs.class", HDFSLocal.class, Operation.class);
    final List<PartitionedSplit>  splits = createSplits();
    assertEquals("Split size should be 1!", 1, splits.size());
    final MockDefaultIO io = new MockDefaultIO();
    io.setConf(conf);
    io.initialize(splits.get(0), new Counters());
    io.writer();
  }  

  @Override
  public void tearDown() throws Exception {
    final java.io.File dir = new java.io.File("/tmp/hama");
    if(dir.exists()) {
      LOG.info("Delete test folder "+dir);
      FileUtils.deleteDirectory(dir);
    }
  }
  
}
