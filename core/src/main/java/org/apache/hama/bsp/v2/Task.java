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
package org.apache.hama.bsp.v2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import org.apache.hama.bsp.TaskAttemptID;

/**
 * A view to task information. 
 * Task building should be done through factory method. 
 */
public final class Task implements Writable {
  
  final Log LOG = LogFactory.getLog(Task.class);

  private TaskAttemptID id;
  private LongWritable startTime;
  private LongWritable finishTime;
  private IntWritable partition;
  private State state;
  private Phase phase = Phase.SETUP;
  private BooleanWritable completed;

  /**
   * Describe in which phase a task is in terms of supersteps.
   * The procedure is
   *             +------------ +
   *             |             |
   *             v             |
   * SETUP -> COMPUTE -> BARRIER_SYNC -> CLEANUP
   */
  public static enum Phase {
    SETUP, COMPUTE, BARRIER_SYNC, CLEANUP
  }

  /**
   * Indicate the current task state ie whether it's running, failed, etc. in a
   * particular executing point.
   */
  public static enum State {
    WAITING, RUNNING, SUCCEEDED, FAILED, RECOVERING, STOPPED, CANCELLED 
  }

  public static final class Builder {

    private TaskAttemptID id;
    private long startTime;
    private long finishTime;
    private int partition;
    private State state;
    private Phase phase = Phase.SETUP;
    private boolean completed;

    public Builder setId(final TaskAttemptID id) {
      this.id = id;
      return this;
    }

    public Builder setStartTime(final long startTime) {
      this.startTime = startTime;
      return this;
    }

    public Builder setFinishTime(final long finishTime) {
      this.finishTime = finishTime;
      return this;
    }

    public Builder setPartition(final int partition) {
      this.partition = partition;
      return this;
    }

    public Builder setState(final State state) {
      this.state = state;
      return this;
    }

    public Builder setPhase(final Phase phase) {
      this.phase = phase;
      return this;
    }

    public Builder setCompleted(final Boolean completed) {
      this.completed = completed;
      return this;
    }

    public Task build() {
      return new Task(id, 
                      startTime, 
                      finishTime, 
                      partition, 
                      state, 
                      phase,
                      completed);
    }
  }

  Task() {} // for Writable

  public Task(final TaskAttemptID id, 
              final long startTime, 
              final long finishTime, 
              final int partition, 
              final State state, 
              final Phase phase, 
              final boolean completed) {
    this.id = id;
    if(null == this.id) 
      throw new IllegalArgumentException("TaskAttemptID not provided.");
    this.startTime = new LongWritable(startTime);
    this.finishTime = new LongWritable(finishTime);
    this.partition = new IntWritable(partition);
    this.state = state;
    if(null == this.state)
      throw new NullPointerException("Task's State is missing!");
    this.phase = phase;
    if(null == this.phase)
      throw new NullPointerException("Task's Phase is missing!");
    this.completed = new BooleanWritable(completed);
  }

  public TaskAttemptID getId() {
    return this.id;
  }

  public long getStartTime() {
    return this.startTime.get();
  }

  public long getFinishTime() {
    return this.finishTime.get();
  }

  public int getPartition() {
    return this.partition.get();
  }

  public State getState() {
    return this.state;
  }

  public Phase getPhase() {
    return this.phase;
  }

  public boolean isCompleted() {
    return this.completed.get();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    this.id.write(out);
    this.startTime.write(out);
    this.finishTime.write(out);
    this.partition.write(out);
    WritableUtils.writeEnum(out, state);
    WritableUtils.writeEnum(out, phase);
    this.completed.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.id = new TaskAttemptID();
    this.id.readFields(in);
    this.startTime = new LongWritable();
    this.startTime.readFields(in);
    this.finishTime = new LongWritable();
    this.finishTime.readFields(in);
    this.partition = new IntWritable();
    this.partition.readFields(in);
    this.state = WritableUtils.readEnum(in, State.class);
    this.phase = WritableUtils.readEnum(in, Phase.class);
    this.completed = new BooleanWritable();
    this.completed.readFields(in);
  }

}

