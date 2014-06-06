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

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.bsp.BSPJobClient.RawSplit;

/**
 * An object that contains partitioned information without holding actual data.
 */
public class PartitionedSplit extends Split {
 
  private static final String local = "file:///";
  private Text splitClassName = new Text("");
  private Text path = new Text(local);
  private ArrayWritable hosts = new ArrayWritable(Text.class);
  private IntWritable partitionId = new IntWritable(1);
  private LongWritable length = new LongWritable(0);

  public PartitionedSplit() {} // for Writable

  /**
   * Initialize PartitionedSplit with related information.
   * See {@link org.apache.hama.bsp.BSPJobClient.RawSplit}.
   * @param splitClass tells which class to split the dataset.
   * @param partitionDir is the directory that stores all split files.
   * @param peerIndex is the index of {@link BSPPeer}.
   * @param hosts denotes the places where the split is stored.
   * @param length denotes the length of this split.
   */
  public PartitionedSplit(final Class<?> splitClass, final String partitionDir, 
                          final int partitionId, final int peerIndex,
                          final String[] hosts, final long length) {
    if(null == splitClass)
      throw new IllegalArgumentException("Split class not provided.");
    this.splitClassName = new Text(splitClass.getName());
    if(null == partitionDir || partitionDir.isEmpty())
      throw new IllegalArgumentException("Partition directory is not set.");
    if(!partitionDir.startsWith("/"))
      throw new IllegalArgumentException("Partition directory is not "+
                                         "sarting from '/'.");
    if(0 > partitionId)
      throw new IllegalArgumentException("Invalid partition id: "+partitionId);
    if(0 > peerIndex)
      throw new IllegalArgumentException("Invalid peer index: "+peerIndex);
    this.path = new Text("%s%s/part-%s/file-%s".format(local, 
                                                       partitionDir, 
                                                       partitionId,
                                                       peerIndex));
    if(null == hosts || 0 == hosts.length) 
      throw new IllegalArgumentException("Invalid hosts setting.");
    this.hosts = new ArrayWritable(hosts); 
    if(0 >= length)
      throw new IllegalArgumentException("Invalid length: "+length);
    this.length.set(length);
  }

  /**
   * Split class name obtained by SplitClass.class.getName().
   * @return String of the split class name.
   */
  public String splitClassName() {
    return splitClassName.toString();
  }

  /**
   * Id for the partition of this split.
   * @return int value for the id.
   */
  public int partitionId() {
    return partitionId.get();
  }

  /**
   * Partition path that store this split.
   * Its value comes from {@link PartitioningRunner}'s partitionDir in a form 
   * of ${partitionDir}/part-${partitionId}/file-${peerIndex}
   * @return Path pointed to the place where stores this split.
   */
  public Path path() {
    return new Path(path.toString());
  }

  @Override
  public long length() {
    return this.length.get();
  }

  @Override
  public String[] hosts() {
    return this.hosts.toStrings();
  }

  @Override 
  public void write(DataOutput out) throws IOException {
    this.splitClassName.write(out);
    this.path.write(out);
    this.hosts.write(out);
    this.partitionId.write(out); 
    this.length.write(out);
  }

  @Override 
  public void readFields(DataInput in) throws IOException {
    this.splitClassName.readFields(in);
    this.path.readFields(in);
    this.hosts.readFields(in);
    this.partitionId.readFields(in); 
    this.length.readFields(in);
  }

  /**
   * Merge {@link BSPJobClient.RawSplit} to this class.
   * N.B.: This function should be removed once BSPJobClient is not needed.
   * @param split is {@link BSPJobClient.RawSplit} data.
   */
  public void merge(RawSplit split) {
    this.splitClassName = new Text(split.getClassName());
    this.path = new Text(split.getPath().toString());
    this.hosts = new ArrayWritable(split.getLocations()); 
    this.partitionId = new IntWritable(split.getPartitionID());
    this.length = new LongWritable(split.getDataLength());
  }
}
