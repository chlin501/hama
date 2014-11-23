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

import java.io.IOException
import java.io.InputStream

import org.apache.hadoop.fs.FSDataInputStream
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.apache.hama.HamaConfiguration

sealed trait KeyValue[K, V]
case class Pair[K, V](key: K, value: V) extends KeyValue[K, V]

class LineReader(in: InputStream, bufferSize: Int) 
      extends org.apache.hadoop.util.LineReader(in, bufferSize) {

  def this(in: InputStream) = this(in, (64 * 1024)) 

  def this(in: InputStream, conf: HamaConfiguration) = 
    this(in, conf.getInt("io.file.buffer.size", (64 * 1024)))
  
}

object LineRecordReader {

  def apply(conf: HamaConfiguration, 
            split: PartitionedSplit): LineRecordReader = {
    val maxLineLength = 
      conf.getInt("bsp.linerecordreader.maxlength", Int.MaxValue)
    var start = split.start
    var end = start + split.length
    val file = split.path
    val compressionCodecs = new CompressionCodecFactory(conf)
    val codec = compressionCodecs.getCodec(new Path(file))

    // open the file and seek to the start of the split
    val fs = new Path(file).getFileSystem(conf)
    val fileIn = fs.open(new Path(split.path))
    var lineReader: LineReader = null
    var skipFirstLine = false
    codec match {
      case null => {
        if (start != 0) {
          skipFirstLine = true
          start -= 1
          fileIn.seek(start)
        }
        lineReader = new LineReader(fileIn, conf)
      }
      case _ => {
        lineReader = new LineReader(codec.createInputStream(fileIn), conf)
        end = Long.MaxValue
      }
    }
    if (skipFirstLine) { // skip first line and re-establish "start".
      start += lineReader.readLine(new Text(), 0, Math.min(Int.MaxValue, (end - start).asInstanceOf[Int]))
    }
    val pos = start
    new LineRecordReader(start, pos, end, Option(lineReader), maxLineLength) 
  }

  def apply(in: InputStream, offset: Long, endOffset: Long, 
            maxLineLength: Int): LineRecordReader = 
    new LineRecordReader(offset, offset, endOffset, Option(new LineReader(in)),
                         maxLineLength)

  def apply(in: InputStream, offset: Long, endOffset: Long, 
            conf: HamaConfiguration): LineRecordReader = {
    val maxLineLength = 
      conf.getInt("bsp.linerecordreader.maxlength", Int.MaxValue)
    new LineRecordReader(offset, offset, endOffset, 
                         Option(new LineReader(in, conf)), maxLineLength)
  }
  
}

class LineRecordReader(initStart: Long, initPos: Long, initEnd: Long, 
                       in: Option[LineReader], maxLineLength: Int) 
      extends RecordReader[KeyValue[LongWritable, Text]] {

  protected var start: Long = initStart
  protected var pos: Long = initPos
  protected var end: Long = initEnd
  protected var lineReader: Option[LineReader] = in 

  protected def readTo(value: Text): Int = lineReader.map { (reader) => 
    reader.readLine(value, maxLineLength, 
                    Math.max(Math.min(Int.MaxValue, 
                                      (end - pos).asInstanceOf[Int]), 
                             maxLineLength)) 
  }.getOrElse(0)

  protected def loop(cond: => Boolean)(key: LongWritable)
                    (f: => Int): Unit = if(cond) {
    key.set(pos) 
    val newSize = f
    if(0 != newSize) {
      pos += newSize
      if(newSize > maxLineLength) {
        loop(cond)(key)(f)
      }
    }
  }

  /** Read a line. */
  override def next(): KeyValue[LongWritable, Text] = {
    val key: LongWritable = new LongWritable(0)
    val value: Text = new Text("")
    loop(pos < end)(key)(readTo(value))
    Pair[LongWritable, Text](key, value) 
  }

  override def close() = lineReader.map { (reader) => reader.close }

}
