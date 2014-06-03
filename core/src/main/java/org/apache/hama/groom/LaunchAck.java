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

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.TaskAttemptID;

public final class LaunchAck implements Writable {

  private IntWritable slotSeq = new IntWritable(0);
  private TaskAttemptID taskAttemptId;

  public LaunchAck() {}

  public LaunchAck(final int slotSeq, final TaskAttemptID taskAttemptId) {
    if(0 >= slotSeq) 
      throw new IllegalArgumentException("Invalid slot seq value: "+slotSeq);
    this.slotSeq.set(slotSeq);
    if(null == taskAttemptId)
      throw new IllegalArgumentException("TaskAttemptID not provided.");
    this.taskAttemptId = taskAttemptId;
  }
  
  public final int slotSeq() {
    return this.slotSeq.get();
  }

  public final TaskAttemptID taskAttemptId() {
    return this.taskAttemptId;
  }

  @Override 
  public void write(DataOutput out) throws IOException {
    this.slotSeq.write(out);
    this.taskAttemptId.write(out);
  }

  @Override 
  public void readFields(DataInput in) throws IOException {
    this.slotSeq = new IntWritable(0);
    this.slotSeq.readFields(in);
    this.taskAttemptId = new TaskAttemptID();
    this.taskAttemptId.readFields(in);
  }

  @Override
  public String toString() {
    return "LaunchAck("+slotSeq()+ ","+taskAttemptId().toString()+")";
  }

}
