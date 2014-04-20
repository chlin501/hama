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
import org.apache.hadoop.io.BytesWritable;
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
  private BSPJobClient.RawSplit split; // TODO: neutral split interface in the future.

  private IntWritable currentSuperstep = new IntWritable(1);

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

  /**
   * Denote the total tasks a job would have. This value won't hanged once a
   * job finishes initialization. Default value is aligned to 
   * <b>bsp.peer.num</b>, which is 1.
   */
  private IntWritable totalBSPTasks = new IntWritable(1);
 
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
    private int currentSuperstep = 1;
    private State state = State.WAITING;
    private Phase phase = Phase.SETUP;
    private boolean completed = false;
    private Marker marker = new Marker(false, ""); 
    private int totalBSPTasks = 1;

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

    public Builder setCurrentSuperstep(final int currentSuperstep) {
      if(0 >= currentSuperstep)
        throw new IllegalArgumentException("Invalid superstep "+
                                           currentSuperstep+"value!");
      this.currentSuperstep = currentSuperstep;
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

    public Builder setTotalBSPTasks(final int totalBSPTasks) {
      this.totalBSPTasks = totalBSPTasks;
      return this;
    }

    public Task build() {
      return new Task(id, 
                      startTime, 
                      finishTime, 
                      partition, 
                      split, 
                      currentSuperstep,
                      state, 
                      phase,
                      completed, 
                      marker,
                      totalBSPTasks);
    }
  }

  public Task() {} // for Writable

  public Task(final TaskAttemptID id, 
              final long startTime, 
              final long finishTime, 
              final int partition, 
              final BSPJobClient.RawSplit split, 
              final int currentSuperstep,
              final State state, 
              final Phase phase, 
              final boolean completed, 
              final Marker marker,
              final int totalBSPTasks) {
    this.id = id;
    if(null == this.id) 
      throw new IllegalArgumentException("TaskAttemptID not provided.");
    this.startTime = new LongWritable(startTime);
    this.finishTime = new LongWritable(finishTime);
    this.partition = new IntWritable(partition);
    this.split = split;
    if(null == this.split)  // it's reasonable that a split is null
      LOG.warn("Split for "+this.id.toString()+" is null.");
    if(0 >= currentSuperstep)
      throw new IllegalArgumentException("Invalid superstep "+currentSuperstep+
                                         " value!");
    this.currentSuperstep = new IntWritable(currentSuperstep);
    this.state = state;
    if(null == this.state)
      throw new NullPointerException("Task's State is missing!");
    this.phase = phase;
    if(null == this.phase)
      throw new NullPointerException("Task's Phase is missing!");
    this.completed = new BooleanWritable(completed);
    this.marker = marker;
    if(0 >= totalBSPTasks)
      throw new IllegalArgumentException("Invalid total bsp tasks value.");
    this.totalBSPTasks = new IntWritable(totalBSPTasks);
  }

  public TaskAttemptID getId() {
    return this.id;
  }

  public long getStartTime() {
    return this.startTime.get();
  }

  /**
   * Set this task's start time to System.currentTimeMillis().
   */
  public void markTaskStarted() {
    this.startTime = new LongWritable(System.currentTimeMillis());
  } 

  public long getFinishTime() {
    return this.finishTime.get();
  }

  /**
   * Set this task's finish time to System.currentTimeMillis().
   */
  public void markTaskFinished() {
    this.finishTime = new LongWritable(System.currentTimeMillis());
  }

  public int getPartition() {
    return this.partition.get();
  }

  public BSPJobClient.RawSplit getSplit() {
    return this.split;
  }

  public int getCurrentSuperstep() {
    return this.currentSuperstep.get();
  }
  
  /**
   * Increment the current superstep via 1.
   */
  public void increatmentSuperspte() {
    this.currentSuperstep = new IntWritable((getCurrentSuperstep()+1));
  }

  public State getState() {
    return this.state;
  }

  /**
   * Transfer the task to the next state.
   */
  public void nextState() {
//TODO:
    throw new UnsupportedOperationException();
  }

  public Phase getPhase() {
    return this.phase;
  }

  public void nextPhase() {
//TODO:
    throw new UnsupportedOperationException();
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

  public int getTotalBSPTasks() {
    return this.totalBSPTasks.get();
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
    this.currentSuperstep.write(out);
    WritableUtils.writeEnum(out, state);
    WritableUtils.writeEnum(out, phase);
    this.completed.write(out);
    this.marker.write(out);
    this.totalBSPTasks.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.id = new TaskAttemptID();
    this.id.readFields(in);
    this.startTime = new LongWritable(0);
    this.startTime.readFields(in);
    this.finishTime = new LongWritable(0);
    this.finishTime.readFields(in);
    this.partition = new IntWritable(1);
    this.partition.readFields(in);
    if(in.readBoolean()) {
      this.split.readFields(in);
    } else {
      this.split = null;
    } 
    this.currentSuperstep = new IntWritable(1);
    this.currentSuperstep.readFields(in);
    this.state = WritableUtils.readEnum(in, State.class);
    this.phase = WritableUtils.readEnum(in, Phase.class);
    this.completed = new BooleanWritable(false);
    this.completed.readFields(in);
    this.marker = new Marker(false, "");
    this.marker.readFields(in);
    this.totalBSPTasks = new IntWritable(1);
    this.totalBSPTasks.readFields(in);
  }

  @Override
  public String toString() {
    return "TaskId: "+id.toString()+" startTime: "+getStartTime()+
           " finishTime: "+getFinishTime()+" partition: "+
           getPartition()+" state: "+getState().toString()+" phase: "+
           getPhase().toString()+" completed: "+isCompleted()+ " marker:"+ 
           marker.toString()+" totalBSPTasks: "+getTotalBSPTasks();
  }
}

