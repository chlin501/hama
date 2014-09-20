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
package org.apache.hama.groom;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.TaskAttemptID;


/**
 * This indicates which {@link Container} is occupied.
 */
public final class Occupied implements Writable { 

  final Log LOG = LogFactory.getLog(Occupied.class);

  private IntWritable slotSeq = new IntWritable(1);

  private TaskAttemptID id = new TaskAttemptID();
  
  public Occupied(final int slotSeq, final TaskAttemptID id) {
    this.slotSeq = new IntWritable(slotSeq);
    this.id = id;
  }

  public Occupied(final int slotSeq, final String taskAttemptId) {
    this(slotSeq, TaskAttemptID.forName(taskAttemptId));
  }

  public int getSlotSeq() {
    return this.slotSeq.get();
  }

  public TaskAttemptID getTaskAttemptId() {
    return this.id;
  }
  
  @Override
  public void readFields(final DataInput in) throws IOException {
    this.slotSeq = new IntWritable(1);
    this.slotSeq.readFields(in);
    this.id = new TaskAttemptID();
    this.id.readFields(in);
  }

  @Override
  public void write(final DataOutput out) throws IOException {
    this.slotSeq.write(out);
    this.id.write(out);
  }

  @Override
  public boolean equals(final Object another) {
    if (another == this) return true;
    if (null == another) return false;
    if (getClass() != another.getClass()) return false;

    final Occupied s = (Occupied) another;
    if (s.getSlotSeq() != slotSeq.get()) return false;
    if(!s.getTaskAttemptId().equals(id)) return false;

    return true;
  }

  @Override
  public String toString() {
    return "Occupied("+getSlotSeq()+","+getTaskAttemptId().toString()+")";
  }
  
}

