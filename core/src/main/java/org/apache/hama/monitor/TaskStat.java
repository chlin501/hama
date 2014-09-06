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
package org.apache.hama.monitor;

import java.io.DataOutput;
import java.io.DataInput;
import java.io.IOException;

import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.bsp.v2.Task.State;
import org.apache.hama.bsp.v2.Task.Phase;
import org.apache.hama.monitor.metrics.MetricsRecord;

public final class TaskStat implements Writable {

  private TaskAttemptID taskAttemptId;
  private IntWritable superstep;
  private LongWritable startTime;
  private LongWritable finishTime;
  private State taskState;
  private Phase taskPhase;
  private BooleanWritable completed;
 
  public TaskStat(final TaskAttemptID taskAttemptId, final int superstep, 
                  final long startTime, final long finishTime, 
                  final State state, Phase phase, final boolean completed) {
    if(null == taskAttemptId) 
      throw new IllegalArgumentException("Task attempt id can't be null!");
    this.taskAttemptId = taskAttemptId;
    this.superstep = new IntWritable(superstep);
    this.startTime = new LongWritable(startTime);
    this.finishTime = new LongWritable(finishTime);
    if(null == state) 
      throw new IllegalArgumentException("Task.State can't be null!");
    this.taskState = state;
    if(null == phase) 
      throw new IllegalArgumentException("Task.Phase can't be null!");
    this.taskPhase = phase;
    this.completed = new BooleanWritable(completed);
  }

  public final TaskAttemptID getTaskAttemptId() {
    return taskAttemptId;
  }

  public final int getSuperstep() {
    return superstep.get();
  }

  public final long getStartTime() {
    return startTime.get();
  }

  public final long getFinishTime() {
    return finishTime.get();
  }

  public final State getState() {
    return taskState;
  }

  public final Phase getPhase() {
    return taskPhase;
  }

  public final boolean isComplete() {
    return completed.get();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    this.taskAttemptId.write(out);
    this.superstep.write(out);
    this.startTime.write(out);
    this.finishTime.write(out);
    WritableUtils.writeEnum(out, taskState);
    WritableUtils.writeEnum(out, taskPhase);
    this.completed.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.taskAttemptId = new TaskAttemptID();
    this.taskAttemptId.readFields(in);
    this.superstep = new IntWritable(0);
    this.superstep.readFields(in);
    this.startTime = new LongWritable(0);
    this.startTime.readFields(in);
    this.finishTime = new LongWritable(0);
    this.finishTime.readFields(in);
    this.taskState = WritableUtils.readEnum(in, State.class);
    this.taskPhase = WritableUtils.readEnum(in, Phase.class);
    this.completed = new BooleanWritable(false);
    this.completed.readFields(in);
  }
}
