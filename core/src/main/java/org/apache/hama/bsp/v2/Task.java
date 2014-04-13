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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import org.apache.hama.bsp.BSPJobClient;
import org.apache.hama.bsp.BSPJobClient.RawSplit;
import org.apache.hama.bsp.TaskAttemptID;

/**
 * A view to task information. 
 * Task building should be done through factory method. 
 */
public final class Task implements Writable {
  
  final Log LOG = LogFactory.getLog(Task.class);

  /* The unique id for this task, including BSPJobID. */
  private TaskAttemptID id;

  /* Time this task begins. */
  private LongWritable startTime = new LongWritable(0);

  /* Time this task finishes. */  
  private LongWritable finishTime = new LongWritable(0);

  /* The partition of this task. */
  private IntWritable partition = new IntWritable(1);

  /* The input data for this task. */
  private BSPJobClient.RawSplit split;

  /* The state of this task. */
  private State state = State.WAITING;

  /* The phase at which this task is. */
  private Phase phase = Phase.SETUP;

  /* Denote if this task is completed. */
  private BooleanWritable completed = new BooleanWritable(false);

  /** 
   * Denote if this task is assigned and to which GroomServer this task 
   * belongs. 
   */
  private Marker marker = new Marker(false, ""); 

 
  public final static class Marker implements Writable {
    private BooleanWritable assigned = new BooleanWritable(false);
    private Text groomServerName = new Text();
   
    public Marker(final boolean assigned, String groomName) {
      this.assigned = new BooleanWritable(assigned); 
      if(null == groomName)
        this.groomServerName = new Text();
      else 
        this.groomServerName = new Text(groomName);
    }

    public boolean isAssigned() {
      return assigned.get();
    }

    public String getAssignedTarget() {
      return this.groomServerName.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      this.assigned.write(out);
      Text.writeString(out, this.groomServerName.toString());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      this.assigned = new BooleanWritable(false);
      this.assigned.readFields(in);
      this.groomServerName = new Text();
      this.groomServerName = new Text(Text.readString(in));
    }

    @Override
    public String toString() {
      return "task is assigned: "+assigned.toString()+" task is assigned to: "+
             groomServerName.toString();
    }
  }

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
    private long startTime = 0;
    private long finishTime = 0;
    private int partition = 1;
    private BSPJobClient.RawSplit split = null;
    private State state = State.WAITING;
    private Phase phase = Phase.SETUP;
    private boolean completed = false;
    private Marker marker = new Marker(false, ""); 

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

    public Builder setSplit(final BSPJobClient.RawSplit split) {
      this.split = split;
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

    public Builder setMarker(final boolean assigned, final String groom) {
      this.marker = new Marker(assigned, groom);
      return this;
    }

    public Task build() {
      return new Task(id, 
                      startTime, 
                      finishTime, 
                      partition, 
                      split, 
                      state, 
                      phase,
                      completed, 
                      marker);
    }
  }

  public Task() {} // for Writable

  public Task(final TaskAttemptID id, 
              final long startTime, 
              final long finishTime, 
              final int partition, 
              final BSPJobClient.RawSplit split, 
              final State state, 
              final Phase phase, 
              final boolean completed, 
              final Marker marker) {
    this.id = id;
    if(null == this.id) 
      throw new IllegalArgumentException("TaskAttemptID not provided.");
    this.startTime = new LongWritable(startTime);
    this.finishTime = new LongWritable(finishTime);
    this.partition = new IntWritable(partition);
    this.split = split;
    if(null == this.split) 
      LOG.warn("Split for "+this.id.toString()+" is null.");
    this.state = state;
    if(null == this.state)
      throw new NullPointerException("Task's State is missing!");
    this.phase = phase;
    if(null == this.phase)
      throw new NullPointerException("Task's Phase is missing!");
    this.completed = new BooleanWritable(completed);
    this.marker = marker;
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

  public BSPJobClient.RawSplit getSplit() {
    return this.split;
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

  public boolean isAssigned() {
    return this.marker.isAssigned();
  }

  public String getAssignedTarget() {
    return this.marker.getAssignedTarget();
  } 

  public void markAsAssigned() {
    this.marker = new Marker(true, "");
  }

  public void markWithTarget(final String name) {
    this.marker = new Marker(true, name);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    this.id.write(out);
    this.startTime.write(out);
    this.finishTime.write(out);
    this.partition.write(out);
    if(null != this.split) {
      out.writeBoolean(true);
      this.split.write(out);
    } else {
      out.writeBoolean(false);
    }
    WritableUtils.writeEnum(out, state);
    WritableUtils.writeEnum(out, phase);
    this.completed.write(out);
    this.marker.write(out);
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
    if(in.readBoolean()) {
      this.split.readFields(in);
    } else {
      this.split = null;
    } 
    this.state = WritableUtils.readEnum(in, State.class);
    this.phase = WritableUtils.readEnum(in, Phase.class);
    this.completed = new BooleanWritable();
    this.completed.readFields(in);
    this.marker = new Marker(false, "");
    this.marker.readFields(in);
  }

  @Override
  public String toString() {
    return "TaskId: "+id.toString()+" startTime: "+startTime.toString()+
           " finishTime: "+finishTime.toString()+" partition: "+
           partition.toString()+" state: "+state.toString()+" phase: "+
           phase.toString()+" completed: "+completed.toString()+" marker:"+
           marker.toString();
  }
}

