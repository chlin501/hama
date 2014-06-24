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
 
  /* This stores common configuration from BSPPeerContainer. */
  protected HamaConfiguration configuration;

  /* This contains setting sepficit to a v2.Task. */
  protected HamaConfiguration taskConfiguration;

  /** contains split information. */
  protected PartitionedSplit split;
  
  protected Counters counters;  

  /**
   * Initialize IO by tighting reader and writer to a specific task setting,
   * including:
   * - task configuration
   * - split
   * - counters
   */
  public void initialize(final HamaConfiguration taskConf,
                         final PartitionedSplit split, 
                         final Counters counters) {
    this.taskConfiguration = taskConf;
    this.split = split;
    this.counters = counters;
  }

  void validate() {
    if(null == taskConfiguration())
      throw new RuntimeException("Task specific configuration not found!");
    if(null == this.split) 
      throw new RuntimeException("No split is specified!");
    if(null == this.counters) 
      throw new RuntimeException("Counter is missing!");
  }

  @Override
  public void setConf(Configuration conf) {
    this.configuration = (HamaConfiguration)conf; // common conf
  }

  @Override
  public Configuration getConf() {
    return this.configuration; // common conf
  }

  /**
   * This denotes the split size to be processed.
   * @return long value for the split data to be processed.
   */
  @Override
  public long splitSize() {
    return this.split.length();
  }

  /**
   * Configuration specific for a v2.Task.
   * @return HamaConfiguration tight to a particular task.
   */
  public HamaConfiguration taskConfiguration() {
    return this.taskConfiguration;
  }

  /**
   * Common cofiguration from {@link BSPPeerContainer}.
   * @return HamaConfiguration from container.
   */
  public HamaConfiguration configuration() {
    return this.configuration;
  }

  private Counter getCounter(Enum<?> name) { // TODO: move counters related to stats in the future
    return counters.findCounter(name);
  }

  /**
   * 1. Restore {@link InputSplit} from common configuration.
   * 2. Obtain record reader from a specific task configuration. 
   * @return RecordReader contains setting for a specific task.
   */
  @Override
  @SuppressWarnings({ "rawtypes" }) 
  public RecordReader reader() throws IOException {
    validate();
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

  RecordReader taskLineRecordReader(final InputSplit inputSplit) 
      throws IOException { 
    return taskInputFormat().getRecordReader(inputSplit, 
                                             new BSPJob(taskConfiguration()));
  }

  Counter taskInputRecordCounter() {
    return getCounter(BSPPeerImpl.PeerCounter.TASK_INPUT_RECORDS);
  }
 
  Counter ioBytesReadCounter() {
    return getCounter(BSPPeerImpl.PeerCounter.IO_BYTES_READ);
  }

  RecordReader createRecordReader(final InputSplit inputSplit) 
      throws IOException {
    return new TrackedRecordReader(taskLineRecordReader(inputSplit),
                                   taskInputRecordCounter(),
                                   ioBytesReadCounter());
  }

  @SuppressWarnings("rawtypes")
  Class<? extends InputFormat> taskInputClass() {
    return taskConfiguration().getClass("bsp.input.format.class", 
                                        TextInputFormat.class,
                                        InputFormat.class);
  }
  
  @SuppressWarnings({ "rawtypes" })
  InputFormat taskInputFormat() {
    return ReflectionUtils.newInstance(taskInputClass(), taskConfiguration());
  }
  
  String childPath(final int partition) {
    return "part-" + formatter.format(partition);
  }

  /**
   * Obtain output directory "bsp.output.dir" from common configuration; 
   * and setup child path tight to a partition id derived from a particular 
   * task.
   * @param timestamp as default temp directory if not output directory found. 
   * @param partitionId is for a particular task.
   */
  Path outputPath(long timestamp, final int partitionId) {
    final String parentPath = configuration().get("bsp.output.dir", 
                                                  "tmp-" + timestamp);
    if(LOG.isDebugEnabled()) LOG.debug("Output parent path is "+parentPath);
    return new Path(parentPath, childPath(partitionId));
  }
 
  /**
   * Output class is tight to a specific task.
   */
  Class<? extends OutputFormat> taskOutputClass() {
    return taskConfiguration().getClass("bsp.output.format.class",
                                       TextOutputFormat.class,
                                       OutputFormat.class);
  }

  /**
   * OutputFormat is tight to a particular {@link Task}.
   * @return OutputFormat configured for a sepcific task.
   */
  @SuppressWarnings("rawtypes")
  OutputFormat taskOutputFormat() {
    return ReflectionUtils.newInstance(taskOutputClass(), taskConfiguration());
  }

  /**
   * Line record writer is tight to a special task.
   * N.B.: taskOutputClass() also needs to define "bsp.output.dir" as well.
   *       otherwise NPE will be thrown because no default is configured in
   *       <b>taskConfiguration</b> variable.
   * @param outPath is the output directory to be used by the writer.
   */
  <K2, V2> RecordWriter<K2, V2> taskLineRecordWriter(final String outPath) 
      throws IOException { 
    return taskOutputFormat().getRecordWriter(null, 
                                              new BSPJob(taskConfiguration()),
                                              outPath);
  } 

  @SuppressWarnings("rawtypes")
  <K2, V2> OutputCollector<K2, V2> outputCollector(final String outPath) 
      throws IOException {
    final RecordWriter<K2, V2> writer = taskLineRecordWriter(outPath); 
    return new OutputCollector<K2, V2>() {
      @Override
      public void collect(K2 key, V2 value) throws IOException {
        writer.write(key, value);
      }
    };
  }

  /**
   * 1. Obtain output path String from common configuration.
   * 2. Obtain output collector from specific task configuration.
   * @return OutputCollector for a specific task.
   */
  @Override
  @SuppressWarnings("rawtypes") 
  public OutputCollector writer() throws IOException {
    validate();
    final long timestamp = System.currentTimeMillis();
    final Path dir = outputPath(timestamp, split.partitionId());
    final String output = OperationFactory.get(configuration()). // common conf
                                           makeQualified(dir);
    if(LOG.isDebugEnabled()) LOG.debug("Writer's output path "+output);
    return outputCollector(output); 
  }
}
