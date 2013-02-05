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

package org.apache.hama.bsp;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;

/**
 * A sub-collection of input files. Unlike
 * {@link org.apache.hadoop.mapred.FileSplit}, CombineFileSplit class does not
 * represent a split of a file, but a split of input files into smaller sets. A
 * split may contain blocks from different file but all the blocks in the same
 * split are probably local to some rack <br>
 * CombineFileSplit can be used to implement
 * {@link org.apache.hadoop.mapred.RecordReader}'s, with reading one record per
 * file.
 * 
 * @see org.apache.hadoop.mapred.FileSplit
 * @see CombineFileInputFormat
 */
public class CombineFileSplit implements InputSplit {

  private Path[] paths;
  private long[] startoffset;
  private long[] lengths;
  private String[] locations;
  private long totLength;
  private BSPJob job;

  /**
   * default constructor
   */
  public CombineFileSplit() {
  }

  public CombineFileSplit(BSPJob job, Path[] files, long[] start,
      long[] lengths, String[] locations) {
    initSplit(job, files, start, lengths, locations);
  }

  public CombineFileSplit(BSPJob job, Path[] files, long[] lengths) {
    long[] startoffset = new long[files.length];
    for (int i = 0; i < startoffset.length; i++) {
      startoffset[i] = 0;
    }
    String[] locations = new String[files.length];
    for (int i = 0; i < locations.length; i++) {
      locations[i] = "";
    }
    initSplit(job, files, startoffset, lengths, locations);
  }

  private void initSplit(BSPJob job, Path[] files, long[] start,
      long[] lengths, String[] locations) {
    this.job = job;
    this.startoffset = start;
    this.lengths = lengths;
    this.paths = files;
    this.totLength = 0;
    this.locations = locations;
    for (long length : lengths) {
      totLength += length;
    }
  }

  public Configuration getJob() {
    return job.getConfiguration();
  }

  @Override
  public long getLength() {
    return totLength;
  }

  /** Returns an array containing the startoffsets of the files in the split */
  public long[] getStartOffsets() {
    return startoffset;
  }

  /** Returns an array containing the lengths of the files in the split */
  public long[] getLengths() {
    return lengths;
  }

  /** Returns the start offset of the i<sup>th</sup> Path */
  public long getOffset(int i) {
    return startoffset[i];
  }

  /** Returns the length of the i<sup>th</sup> Path */
  public long getLength(int i) {
    return lengths[i];
  }

  /** Returns the number of Paths in the split */
  public int getNumPaths() {
    return paths.length;
  }

  /** Returns the i<sup>th</sup> Path */
  public Path getPath(int i) {
    return paths[i];
  }

  /** Returns all the Paths in the split */
  public Path[] getPaths() {
    return paths;
  }

  /** Returns all the Paths where this input-split resides */
  @Override
  public String[] getLocations() throws IOException {
    return locations;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    totLength = in.readLong();
    int arrLength = in.readInt();
    lengths = new long[arrLength];
    for (int i = 0; i < arrLength; i++) {
      lengths[i] = in.readLong();
    }
    int filesLength = in.readInt();
    paths = new Path[filesLength];
    for (int i = 0; i < filesLength; i++) {
      paths[i] = new Path(Text.readString(in));
    }
    arrLength = in.readInt();
    startoffset = new long[arrLength];
    for (int i = 0; i < arrLength; i++) {
      startoffset[i] = in.readLong();
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(totLength);
    out.writeInt(lengths.length);
    for (long length : lengths) {
      out.writeLong(length);
    }
    out.writeInt(paths.length);
    for (Path p : paths) {
      Text.writeString(out, p.toString());
    }
    out.writeInt(startoffset.length);
    for (long length : startoffset) {
      out.writeLong(length);
    }
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    for (int i = 0; i < paths.length; i++) {
      if (i == 0) {
        sb.append("Paths:");
      }
      sb.append(paths[i].toUri().getPath()).append(":").append(startoffset[i]).append("+").append(lengths[i]);
      if (i < paths.length - 1) {
        sb.append(",");
      }
    }
    if (locations != null) {
      String locs = "";
      StringBuffer locsb = new StringBuffer();
        for (String location : locations) {
            locsb.append(location).append(":");
        }
      locs = locsb.toString();
      sb.append(" Locations:").append(locs).append("; ");
    }
    return sb.toString();
  }
}
