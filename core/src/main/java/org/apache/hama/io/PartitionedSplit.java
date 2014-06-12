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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.bsp.BSPJobClient.RawSplit;


/**
 * A replacement for {@link BSPJobClient.RawSplit}.
 */
public class PartitionedSplit extends Split {

  public static final Log LOG = LogFactory.getLog(PartitionedSplit.class);
 
  private Text splitClassName = new Text("");
  private ArrayWritable hosts = new ArrayWritable(Text.class);
  private IntWritable partitionId = new IntWritable(1);
  private LongWritable length = new LongWritable(0);
  /* This represents the split binary data, generally be FileSplit. */
  private BytesWritable bytes = new BytesWritable();

  public PartitionedSplit() {} // for Writable

  /**
   * Initialize PartitionedSplit with related information.
   * See {@link org.apache.hama.bsp.BSPJobClient.RawSplit}.
   * @param splitClass tells which {@link InputSplit} class it represents.
   * @param partitionId denotes the id of partition for this split holds.
   * @param hosts denotes the places where the split is stored.
   * @param length denotes the length of this split.
   * @param bytes is the binary data representation of InputSplit, generally
   *              it's FileSplit.
   */
  public PartitionedSplit(final Class<?> splitClass, final int partitionId, 
                          final String[] hosts, final long length, 
                          final byte[] bytes, final int offset, 
                          int bytesLength) {
    if(null == splitClass)
      throw new IllegalArgumentException("Split class not provided.");
    this.splitClassName = new Text(splitClass.getName());
    if(LOG.isDebugEnabled())
      LOG.debug("Split class name is "+splitClassName.toString());

    if(0 > partitionId)
      throw new IllegalArgumentException("Invalid partition id: "+partitionId);
    this.partitionId.set(partitionId);
    if(LOG.isDebugEnabled()) 
      LOG.debug("Partition id is "+this.partitionId.get());

    if(null == hosts || 0 == hosts.length) 
      throw new IllegalArgumentException("Invalid hosts setting.");
    final Text[] texts = new Text[hosts.length];
    for(int idx = 0; idx < hosts.length; idx++) {
      texts[idx] = new Text(hosts[idx]); 
      if(LOG.isDebugEnabled()) 
        LOG.debug("host["+idx+"]:"+texts[idx].toString());
    }
    this.hosts.set(texts);
    if(0 >= length)
      throw new IllegalArgumentException("Invalid length: "+length);
    this.length.set(length);
    if(LOG.isDebugEnabled()) LOG.debug("length value is "+this.length.get());

    if(null == bytes)
      throw new IllegalArgumentException("InputSplit binary data not found!");
    this.bytes.set(bytes, offset, bytesLength);
    if(LOG.isDebugEnabled()) 
      LOG.debug("bytes length is "+this.bytes.get().length);
  }

  /**
   * InputSplit class name, generally it's {@link FileSplit}.
   * See {@link BSPJobClient#writeSplits}.
   * @return String of the split class name.
   */
  public String splitClassName() {
    return splitClassName.toString();
  }

  /**
   * The id of partition for this split.
   * @return int value for the id.
   */
  public int partitionId() {
    return partitionId.get();
  }

  @Override
  public long length() {
    return this.length.get();
  }

  @Override
  public String[] hosts() {
    return this.hosts.toStrings();
  }

  public byte[] bytes() {
    return this.bytes.get();
  }

  @Override 
  public void write(DataOutput out) throws IOException {
    this.splitClassName.write(out);
    this.hosts.write(out);
    this.partitionId.write(out);
    this.length.write(out);
    this.bytes.write(out);
  }

  @Override 
  public void readFields(DataInput in) throws IOException {
    this.splitClassName.readFields(in);
    this.hosts.readFields(in);
    this.partitionId.readFields(in);
    this.length.readFields(in);
    this.bytes.readFields(in);
  }

  /**
   * Merge {@link BSPJobClient.RawSplit} to this class without actual 
   * {@link ByteWritable} data.
   * N.B.: This function should be removed once BSPJobClient is not needed.
   * @param split is {@link BSPJobClient.RawSplit} data.
   */
  public void merge(RawSplit split) {
    this.splitClassName = new Text(split.getClassName());
    this.hosts = new ArrayWritable(split.getLocations()); 
    this.partitionId = new IntWritable(split.getPartitionID());
    this.length = new LongWritable(split.getDataLength());
    this.bytes = split.getBytes();
  }
}
