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

import java.io.IOException;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.bsp.BSPPeerImpl; // TODO: 
import org.apache.hama.bsp.Counters; // TODO: 
import org.apache.hama.bsp.Counters.Counter; // TODO: 
import org.apache.hama.bsp.FileSplit;
import org.apache.hama.bsp.InputFormat;
import org.apache.hama.bsp.InputSplit;
import org.apache.hama.bsp.OutputCollector;
import org.apache.hama.bsp.RecordReader;
import org.apache.hama.bsp.TextInputFormat;
import org.apache.hama.bsp.TrackedRecordReader;
import org.apache.hama.io.IO;
import org.apache.hama.HamaConfiguration;

public class DefaultIO implements IO<RecordReader, OutputCollector> {

  private final HamaConfiguration configuration;

  /** contains split information. */
  private final PartitionedSplit split;
  
  private final Counters counters;  // TODO: move to monitor stats in the future

  public DefaultIO(final HamaConfiguration conf, 
                   final PartitionedSplit split, 
                   final Counters counters) {
    if(null == conf) 
      throw new IllegalArgumentException("HamaConfiguration not provided!");
    this.configuration = conf;
    if(null == split) 
      throw new IllegalArgumentException("No split found!");
    this.split = split;
    if(null == counters) 
      throw new IllegalArgumentException("Counter is missing!");
    this.counters = counters;
  }

  public HamaConfiguration configuration() {
    return this.configuration;
  }

  Counter getCounter(Enum<?> name) { // TODO: move counters related to stats in the future
    return counters.findCounter(name);
  }

  @Override
  @SuppressWarnings({ "rawtypes" }) 
  public RecordReader reader() throws IOException {
    InputSplit inputSplit = null;
    try {
      if(null != split.splitClassName()) {
        inputSplit = (InputSplit) ReflectionUtils.newInstance(
                     configuration().getClassByName(split.splitClassName()), 
                     configuration());     
      }
    } catch (ClassNotFoundException cnfe) {
      throw new RuntimeException("Split class "+split.splitClassName()+
                                 " not found!", cnfe);
    }
    RecordReader reader = null;
    if (null != inputSplit) {
      final DataInputBuffer splitBuffer = new DataInputBuffer();
      splitBuffer.reset(split.bytes(), 0, (int)split.length());
      inputSplit.readFields(splitBuffer);
      reader = new TrackedRecordReader(
        ((TextInputFormat)defaultInputFormat()).recordReader(configuration(), 
                                                             inputSplit),
        getCounter(BSPPeerImpl.PeerCounter.TASK_INPUT_RECORDS),
        getCounter(BSPPeerImpl.PeerCounter.IO_BYTES_READ));
    }
    return reader;
  }
  
  @SuppressWarnings({ "rawtypes" })
  private InputFormat defaultInputFormat() {
    return ReflectionUtils.
           newInstance(configuration().getClass("bsp.input.format.class", 
                                                TextInputFormat.class,
                                                InputFormat.class), 
                       configuration());
  }
  
 
  @Override
  @SuppressWarnings("rawtypes") 
  public OutputCollector writer() throws IOException {
    return null; // xxxx
  }
}
