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
import java.text.NumberFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.bsp.BSPJob; // TODO: refactor io to get rid of this 
import org.apache.hama.bsp.BSPPeerImpl; // TODO: move to monitor stats
import org.apache.hama.bsp.Counters; // TODO: move to monitor stats 
import org.apache.hama.bsp.Counters.Counter; // TODO: move to monitor stats
import org.apache.hama.bsp.FileSplit;
import org.apache.hama.bsp.InputFormat;
import org.apache.hama.bsp.InputSplit;
import org.apache.hama.bsp.OutputCollector;
import org.apache.hama.bsp.OutputFormat;
import org.apache.hama.bsp.RecordReader;
import org.apache.hama.bsp.RecordWriter;
import org.apache.hama.bsp.TextInputFormat;
import org.apache.hama.bsp.TrackedRecordReader;
import org.apache.hama.bsp.TextOutputFormat;
import org.apache.hama.fs.Operation;
import org.apache.hama.fs.OperationFactory;
import org.apache.hama.io.IO;
import org.apache.hama.HamaConfiguration;

// TODO: counter should be moved to monitor stats and recorded in zk.
public class DefaultIO implements IO<RecordReader, OutputCollector> {

  private static final NumberFormat formatter = NumberFormat.getInstance();
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

  /**
   * This denotes the split size to be processed.
   * @return long value for the split data to be processed.
   */
  @Override
  public long splitSize() {
    return this.split.length();
  }

  public HamaConfiguration configuration() {
    return this.configuration;
  }

  private Counter getCounter(Enum<?> name) { // TODO: move counters related to stats in the future
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
      reader = createRecordReader(inputSplit);
    }
    return reader;
  }

  RecordReader lineRecordReader(final InputSplit inputSplit) 
      throws IOException { 
    return defaultInputFormat().getRecordReader(inputSplit, 
                                                new BSPJob(configuration()));
  }

  Counter taskInputRecordCounter() {
    return getCounter(BSPPeerImpl.PeerCounter.TASK_INPUT_RECORDS);
  }
 
  Counter ioBytesReadCounter() {
    return getCounter(BSPPeerImpl.PeerCounter.IO_BYTES_READ);
  }

  RecordReader createRecordReader(final InputSplit inputSplit) 
      throws IOException {
    return new TrackedRecordReader(lineRecordReader(inputSplit),
                                   taskInputRecordCounter(),
                                   ioBytesReadCounter());
  }

  @SuppressWarnings("rawtypes")
  Class<? extends InputFormat> inputClass() {
    return configuration().getClass("bsp.input.format.class", 
                                    TextInputFormat.class,
                                    InputFormat.class);
  }
  
  @SuppressWarnings({ "rawtypes" })
  InputFormat defaultInputFormat() {
    return ReflectionUtils.newInstance(inputClass(), configuration());
  }
  
  String outputChildPath(final int partition) {
    return "part-" + formatter.format(partition);
  }

  Path outputPath(final long timestamp, final int partitionId) {
    return new Path(configuration().get("bsp.output.dir", "tmp-" + timestamp), 
                                        outputChildPath(partitionId));
  }

  Class<? extends OutputFormat> outputClass() {
    return configuration().getClass("bsp.output.format.class",
                                    TextOutputFormat.class,
                                    OutputFormat.class);
  }

  @SuppressWarnings("rawtypes")
  OutputFormat defaultOutputFormat() {
    return ReflectionUtils.newInstance(outputClass(), configuration());
  }

  <K2, V2> RecordWriter<K2, V2> lineRecordWriter(final String outputPath) 
      throws IOException { 
    return defaultOutputFormat().getRecordWriter(null, 
                                                new BSPJob(configuration()),
                                                outputPath);
  } 

  @SuppressWarnings("rawtypes")
  <K2, V2> OutputCollector<K2, V2> outputCollector(final String outputPath) 
      throws IOException {
    final RecordWriter<K2, V2> writer = lineRecordWriter(outputPath);
    return new OutputCollector<K2, V2>() {
      @Override
      public void collect(K2 key, V2 value) throws IOException {
        writer.write(key, value);
      }
    };
  }

  @Override
  @SuppressWarnings("rawtypes") 
  public OutputCollector writer() throws IOException {
    String output = null;
    if (null != configuration().get("bsp.output.dir")) {
      final long timestamp = System.currentTimeMillis();
      final Path dir = outputPath(timestamp, split.partitionId());
      output = OperationFactory.get(configuration()).makeQualified(dir);
    }
    return outputCollector(output); 
  }
}
