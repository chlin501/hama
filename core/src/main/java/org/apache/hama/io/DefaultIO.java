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
import java.io.DataInputStream;
import java.io.IOException;
import java.text.NumberFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.bsp.BSPJob; // TODO: refactor io to get rid of this 
import org.apache.hama.bsp.BSPPeerImpl; 
import org.apache.hama.bsp.Counters; 
import org.apache.hama.bsp.Counters.Counter; 
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
//       bsp peer interface provides getStat which has access to counter.
public class DefaultIO implements IO<RecordReader, OutputCollector>,
                                  Configurable {

  static final Log LOG = LogFactory.getLog(DefaultIO.class);

  private static final NumberFormat formatter = NumberFormat.getInstance();
  private HamaConfiguration configuration;

  /** contains split information. */
  protected PartitionedSplit split;
  
  private Counters counters;  

/*
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
*/

  public void initialize(final PartitionedSplit split, 
                         final Counters counters) {
    if(null == split) 
      throw new IllegalArgumentException("No split found!");
    this.split = split;
    if(null == counters) 
      throw new IllegalArgumentException("Counter is missing!");
    this.counters = counters;
  }

  @Override
  public void setConf(Configuration conf) {
    this.configuration = (HamaConfiguration)conf;
  }

  @Override
  public Configuration getConf() {
    return this.configuration;
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
      throw new IOException("Split class "+split.splitClassName()+
                                 " not found!", cnfe);
    }

    RecordReader reader = null;
    if (null != inputSplit) {
      if(LOG.isDebugEnabled())
        LOG.debug(split.getClass().getName()+" stores "+split.bytes().length+
                  " as FileSplit.");
      DataInputStream splitBuffer  = null;
      try {
        final ByteArrayInputStream bin = 
          new ByteArrayInputStream(split.bytes());
        splitBuffer = new DataInputStream(bin);
        inputSplit.readFields(splitBuffer);
        if(null != reader) reader.close();
        reader = createRecordReader(inputSplit);
      } catch (Exception e) {
        throw new IOException("Fail restoring "+inputSplit.getClass().getName()+
                              "from "+split.getClass().getName(), e);
      } finally {
        splitBuffer.close();
      }
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
  
  String childPath(final int partition) {
    return "part-" + formatter.format(partition);
  }

  Path outputPath(long timestamp, final int partitionId) {
    final String parentPath = configuration().get("bsp.output.dir", 
                                                  "tmp-" + timestamp);
    if(LOG.isDebugEnabled()) LOG.debug("Output parent path is "+parentPath);
    return new Path(parentPath, childPath(partitionId));
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

  <K2, V2> RecordWriter<K2, V2> lineRecordWriter(final String outPath) 
      throws IOException { 
    return defaultOutputFormat().getRecordWriter(null, 
                                                new BSPJob(configuration()),
                                                outPath);
  } 

  @SuppressWarnings("rawtypes")
  <K2, V2> OutputCollector<K2, V2> outputCollector(final String outPath) 
      throws IOException {
    final RecordWriter<K2, V2> writer = lineRecordWriter(outPath);
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
    final long timestamp = System.currentTimeMillis();
    final Path dir = outputPath(timestamp, split.partitionId());
    final String output = OperationFactory.get(configuration()).
                                           makeQualified(dir);
    if(LOG.isDebugEnabled()) LOG.debug("Writer's output path "+output);
    return outputCollector(output); 
  }
}
