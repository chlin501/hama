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
package org.apache.hama.io
/*
import java.io.ByteArrayInputStream
import java.io.DataInputStream
import java.io.IOException
import java.text.NumberFormat
import org.apache.hadoop.conf.Configurable
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.DataInputBuffer
import org.apache.hadoop.util.ReflectionUtils
import org.apache.hama.bsp.BSPJob // TODO: refactor io to get rid of this 
import org.apache.hama.bsp.BSPPeerImpl
import org.apache.hama.bsp.Counters 
import org.apache.hama.bsp.Counters.Counter
import org.apache.hama.bsp.FileSplit
import org.apache.hama.bsp.InputFormat
import org.apache.hama.bsp.InputSplit
import org.apache.hama.bsp.OutputCollector
import org.apache.hama.bsp.OutputFormat
import org.apache.hama.bsp.RecordReader
import org.apache.hama.bsp.RecordWriter
import org.apache.hama.bsp.TextInputFormat
import org.apache.hama.bsp.TrackedRecordReader
import org.apache.hama.bsp.TextOutputFormat
import org.apache.hama.fs.Operation
import org.apache.hama.HamaConfiguration
import org.apache.hama.logging.CommonLog

object DefaultIO {

  val formatter = NumberFormat.getInstance()

}

// TODO: counter should be moved to monitor stats and recorded in zk.
//       bsp peer interface provides getStat which has access to counter.
class DefaultIO extends IO[RecordReader[_, _], OutputCollector[_, _]] 
                with Configurable 
                with CommonLog {

  import DefaultIO._
 
  // This stores common configuration from Container. /
  protected var commonConf: HamaConfiguration = _

  // This contains setting sepficit to a v2.Task. /
  protected var taskConf: HamaConfiguration = _

  // contains split information./
  //protected var split: PartitionedSplit = _
  
  protected var counters: Counters = _

  // this will only available after reader() gets called. /
  private var splitLength: Long = -1

   * Initialize IO by tighting reader and writer to a specific task setting,
   * including:
   * - task configuration
   * - split
   * - counters
  def initialize(taskConf: HamaConfiguration, split: FileSplit, 
                 counters: Counters) {
    this.taskConf = taskConf
    //this.split = split
    this.counters = counters
  }

  private def validate() {
    if(null == taskConfiguration())
      throw new RuntimeException("Task specific configuration not found!")
    //if(null == this.split) 
      //throw new RuntimeException("No split is specified!")
    if(null == this.counters) 
      throw new RuntimeException("Counter is missing!")
  }

   * This passes in common configuration, equivalent to configuration().
   * @param conf is the common configuration 
  override def setConf(conf: Configuration) {
    if(null == conf) 
      throw new IllegalArgumentException("Common HamaConfiguration is "+
                                         "missing for "+getClass.getName+"!")
    this.commonConf = conf.asInstanceOf[HamaConfiguration]
  }

   * This returns common configuration, equivalent to configuration().
   * @return Configuration content is the same as configuration().
  override def getConf(): Configuration = this.commonConf

   * This denotes the split content size to be processed. 
   * <b>-1</b> indicates the reader() is not yet initialized.
   * Note this value will only available after reader() gets called.
   * @return long value for the split data to be processed.
  override def splitSize(): Long = this.splitLength

   * Configuration specific for a v2.Task.
   * @return HamaConfiguration tight to a particular task.
  def taskConfiguration(): HamaConfiguration = this.taskConf

   * Common cofiguration from {@link Container}.
   * @return HamaConfiguration from container.
  def configuration(): HamaConfiguration = this.commonConf

  private def getCounter(name: Enum[_]): Counter = counters.findCounter(name)

   * 1. Restore {@link InputSplit} from common configuration.
   * 2. Obtain record reader from a specific task configuration. 
   * @return RecordReader contains setting for a specific task.
  @throws(classOf[IOException])
  override def reader(): RecordReader[_,_] = {
//    validate()
//    var inputSplit: InputSplit = null
//    try {
//      if(null != split.splitClassName()) {
//        inputSplit = ReflectionUtils.newInstance(
//                     configuration().getClassByName(split.splitClassName()), 
//                     configuration()).asInstanceOf[InputSplit]
//      }
//    } catch {
//      case cnfe: ClassNotFoundException =>
//        throw new IOException("Split class "+split.splitClassName()+
//                              " not found!", cnfe)
//    }
//
//    var reader: RecordReader[_,_] = null
//    if (null != inputSplit) { // TODO: refactor to read from path and block location
//      try {
//        inputSplit.readFields(splitBuffer)
//        if(null != reader) reader.close()
//        reader = createRecordReader(inputSplit)
//        this.splitLength = inputSplit.getLength()
//      } catch {
//        case e: Exception =>
//          throw new IOException("Fail restoring "+inputSplit.getClass.getName+
//                                "from "+split.getClass().getName(), e)
//      } finally {
//        splitBuffer.close()
//      }
//    }
    var reader: RecordReader[_,_] = null
    reader
  }

  @throws(classOf[IOException])  
  def taskLineRecordReader(inputSplit: InputSplit): RecordReader[_,_] = 
    taskInputFormat().getRecordReader(inputSplit, 
                                      new BSPJob(taskConfiguration()))

  def taskInputRecordCounter(): Counter = 
    getCounter(BSPPeerImpl.PeerCounter.TASK_INPUT_RECORDS)
 
  def ioBytesReadCounter(): Counter = 
    getCounter(BSPPeerImpl.PeerCounter.IO_BYTES_READ)

  @throws(classOf[IOException]) 
  def createRecordReader(inputSplit: InputSplit): RecordReader[_,_] = 
    new TrackedRecordReader(taskLineRecordReader(inputSplit),
                            taskInputRecordCounter(),
                            ioBytesReadCounter())

  def taskInputClass(): Class[_] = 
    taskConfiguration().getClass("bsp.input.format.class", 
                                 classOf[TextInputFormat],
                                 classOf[InputFormat[_,_]])
  
  def taskInputFormat(): InputFormat[_, _] = 
    ReflectionUtils.newInstance(taskInputClass(), taskConfiguration()).
                    asInstanceOf[InputFormat[_,_]]
  
  def childPath(partition: Int): String = "part-" + formatter.format(partition)

   * Obtain output directory "bsp.output.dir" from common configuration; 
   * and setup child path tight to a partition id derived from a particular 
   * task.
   * @param timestamp as default temp directory if not output directory found. 
   * @param partitionId is for a particular task.
  def outputPath(timestamp: Long, partitionId: Int): Path = {
    val parentPath = configuration().get("bsp.output.dir", 
                                         "tmp-" + timestamp)
    LOG.debug("Output parent path is "+parentPath)
    new Path(parentPath, childPath(partitionId))
  }
 
   * Output class is tight to a specific task.
  def taskOutputClass(): Class[_] = 
    taskConfiguration().getClass("bsp.output.format.class",
                                 classOf[TextOutputFormat[_,_]],
                                 classOf[OutputFormat[_,_]])

   * OutputFormat is tight to a particular {@link Task}.
   * @return OutputFormat configured for a sepcific task.
  def taskOutputFormat[K2, V2](): OutputFormat[K2, V2] = 
    ReflectionUtils.newInstance(taskOutputClass(), taskConfiguration()).
                    asInstanceOf[OutputFormat[K2, V2]]

   * Line record writer is tight to a special task.
   * N.B.: taskOutputClass() also needs to define "bsp.output.dir" as well.
   *       otherwise NPE will be thrown because no default is configured in
   *       <b>taskConfiguration</b> variable.
   * @param outPath is the output directory to be used by the writer.
  @throws(classOf[IOException])  
  def taskLineRecordWriter[K2, V2](outPath: String): RecordWriter[K2, V2] =  
    taskOutputFormat[K2, V2]().getRecordWriter(null, 
                                               new BSPJob(taskConfiguration()),
                                               outPath)

  @throws(classOf[IOException]) 
  def outputCollector[K2, V2](outPath: String): OutputCollector[K2, V2] = {
    val writer = taskLineRecordWriter[K2, V2](outPath)
    new OutputCollector[K2, V2]() {
      @throws(classOf[IOException]) 
      override def collect(key: K2, value: V2) {
        writer.write(key, value)
      }
    }
  }

   * 1. Obtain output path String from common configuration.
   * 2. Obtain output collector from specific task configuration.
   * @return OutputCollector for a specific task.
  @throws(classOf[IOException]) 
  override def writer(): OutputCollector[_, _] = {
//    validate()
//    val timestamp = System.currentTimeMillis()
//    val dir = outputPath(timestamp, split.partitionId())
//    val output = Operation.get(configuration()).makeQualified(dir)
//    LOG.debug("Writer's output path "+output)
//    outputCollector(output)

    null.asInstanceOf[OutputCollector[_, _]]
  }
}
*/
