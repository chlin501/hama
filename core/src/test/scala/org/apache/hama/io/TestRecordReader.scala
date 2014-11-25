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

import java.io.DataOutput
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hama.HamaConfiguration
import org.apache.hama.TestEnv
import org.apache.hama.bsp.BSPJob
import org.apache.hama.bsp.FileInputFormat
import org.apache.hama.bsp.FileSplit
import org.apache.hama.fs.Operation
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.collection.Iterator

@RunWith(classOf[JUnitRunner])
class TestRecordReader extends TestEnv("TestRecordReader") {

  val line1 = "Split and Reader\n"
  val line2 = "A test for checking split and reader functions.\n"
  val line3 = "The first step is to create splits based on the content.\n"
  val line4 = "Second, create record reader from input format.\n"
  val line5 = "Then verify if content is correct.\n"
  val content  = new Text(line1+line2+line3+line4+line5)
  val fileName = "file-1"

  val numTasks = 2
  val blockSize: Long = content.toString.length / numTasks

  val operation = {
    testConfiguration.setLong("fs.local.block.size", blockSize)
    Operation.get(testConfiguration)
  }
  override def beforeAll = {
    super.beforeAll
    val dest = new Path(testRoot.toString, fileName)
    prepare(content, dest)
  }

  def prepare(data: Text, dest: Path) {
    val out = operation.create(dest)
    data.write(out.asInstanceOf[DataOutput])
    out.close
  }

  it("test record reader functions.") {
    val job = new BSPJob
    FileInputFormat.setInputPaths(job, testRootPath)
    val splits = job.getInputFormat.getSplits(job, numTasks)
    LOG.info("How many splits are created? {}", splits.length)
    assert(2 == splits.length)
    val fileSplit = splits(splits.length-1).asInstanceOf[FileSplit]

    val reader = new org.apache.hama.bsp.LineRecordReader(new HamaConfiguration, fileSplit)
    val key = new LongWritable()
    val value = new Text()
    var expected = Map.empty[LongWritable, Text]
    while(reader.next(key, value)) {
      LOG.info("Expected key {} value {}", key, value)
      expected ++= Map(key -> value)
    }

    val split = PartitionedSplit.from(fileSplit)
    val lineRecordReader = LineRecordReader(new HamaConfiguration, split)
    var pair: KeyValue[LongWritable, Text] = EmptyPair
    var actual = Map.empty[LongWritable, Text]
    while({ pair = lineRecordReader.next; !pair.equals(EmptyPair) }) {
      val (k, v) = pair.getOrElse((null, null))
      LOG.info("Actual key: {} value: {} ", k, v) 
      assert(null != k)
      assert(null != v)
      actual ++= Map(k -> v)
    }

    expected.foreach { case (key, value) => actual.get(key) match {
      case None => throw new RuntimeException("Line expected: "+value+" at "+
                                              key)
      case Some(v) => assert(value.equals(v))
    }}
  }
}
