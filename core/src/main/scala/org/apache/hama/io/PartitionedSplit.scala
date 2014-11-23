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

import java.io.DataInput
import java.io.DataOutput
import java.io.IOException

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.ArrayWritable
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.Writable

object PartitionedSplit {

  def apply(path: String, partitionId: Int, start: Long, length: Long): 
    PartitionedSplit = apply(path, partitionId, start, length, Array()) 

  def apply(path: String, partitionId: Int, start: Long, length: Long,
            hosts: Array[String]): PartitionedSplit = {
    val split = new PartitionedSplit()
    split.setPath(path)
    split.setPartitionId(partitionId)
    split.setStart(start)
    split.setLength(length)
    split.setHosts(hosts)
    split
  }

  final def defaultHosts(): ArrayWritable = {
    val w = new ArrayWritable(classOf[Text])
    w.set(Array[Text]().asInstanceOf[Array[Writable]])
    w
  }
}

class PartitionedSplit extends Writable {

  import PartitionedSplit._
 
  protected var p = new Path("")
  protected var pId = new IntWritable(1)
  protected var splitStart = new LongWritable(0)
  protected var splitLength = new LongWritable(0)
  protected var h = defaultHosts()

  protected[io] def setPath(path: String) = path match {
    case null | "" => throw new RuntimeException("Path is not provied!")
    case _ => p = new Path(path)
  }

  protected[io] def setPartitionId(partitionId: Int) = partitionId match {
    case id: Int if id > 0 => pId = new IntWritable(partitionId)
    case v@_ => throw new RuntimeException("Invalide partition id: "+v+"!")
  }

  protected[io] def setStart(start: Long) = start match {
    case s: Long if s > 0 => splitStart = new LongWritable(start)
    case v@_ => throw new RuntimeException("Invalid split start value: "+v)
  }

  protected[io] def setLength(length: Long) = length match {
    case l: Long if l > 0 => splitLength = new LongWritable(length)
    case v@_ => throw new RuntimeException("Invalid split length value: "+v)
  }

  /**
   * Hosts is an array of String that doesn't contain port values.
   */
  protected[io] def setHosts(hosts: Array[String]) = hosts match {
    case null => throw new RuntimeException("Hosts value is empty! ")
    case _ => h = {
      val w = new ArrayWritable(classOf[Text])
      w.set(hosts.asInstanceOf[Array[Writable]])
      w
    }
  }

  def path(): String = p.toString

  def partitionId(): Int = pId.get

  def start(): Long = splitStart.get

  def length(): Long = splitLength.get

  def hosts(): Array[String] = h.toStrings

  @throws(classOf[IOException])
  override def write(out: DataOutput) {
    Text.writeString(out, p.toString)
    pId.write(out)
    splitStart.write(out)
    splitLength.write(out)
    h.write(out)
  }

  @throws(classOf[IOException])
  override def readFields(in: DataInput) {
    p = new Path(Text.readString(in))
    pId.readFields(in)
    splitStart.readFields(in)
    splitLength.readFields(in)
    h.readFields(in)
  }

}

