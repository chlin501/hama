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
package org.apache.hama.groom

import java.io.IOException
import java.io.DataInput
import java.io.DataOutput
import org.apache.hama.monitor.GroomStats
import org.apache.hadoop.io.Writable

sealed trait RequestMessage
case object TaskRequest extends RequestMessage
final class  RequestTask extends Writable with RequestMessage {

  protected[groom] var s: Option[GroomStats] = None

  def stats(): Option[GroomStats] = s

  @throws(classOf[IOException])
  override def write(out: DataOutput) = s match {
    case Some(stats) => {
      out.writeBoolean(true)
      stats.write(out)
    } 
    case None => out.writeBoolean(false)
  }

  @throws(classOf[IOException])
  override def readFields(in: DataInput): Unit = if(in.readBoolean) {
    val stats = new GroomStats
    stats.readFields(in)
    s = Option(stats)
  }
 
}
