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
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.io.PartitionedSplit;

/**
 * A view to task information. 
 * Task building should be done through factory method. 
 */
public final class Task implements Writable {

  final Log LOG = LogFactory.getLog(Task.class);

  /* The unique id for this task, including BSPJobID. */
  private TaskAttemptID id;

  /** 
   * This variable, derived from v2.Job, contains specific setting to this task.
   */
  private HamaConfiguration configuration = new HamaConfiguration();

  // TODO: maybe we need record time when the task in queue?
  /* Time this task begins. */
  private LongWritable startTime = new LongWritable(0);

  /* Time this task finishes. */  
  private LongWritable finishTime = new LongWritable(0);

  /* The input data for this task. */
  private PartitionedSplit split; 

  private IntWritable currentSuperstep = new IntWritable(1);

  /**
   * This is only used for finding next State. Read only.
   */
  public final Stage<State> States = new Stage<State>(State.values());

  /* The state of this task. */
  private State state = States.next(); 

  /**
   * This is only used for finding next Phase. Read only.
   */
  public final Stage<Phase> Phases = new Stage<Phase>(Phase.values());

  /* The phase at which this task is. */
  private Phase phase = Phases.next(); 

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

    /**
     * Tell to which GroomServer this task is dispatched.
     * @return String is the name of target GroomServer.
     */
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
      return "Marker("+assigned.toString()+","+
             groomServerName.toString()+")";
    }
  }

  /**
   * Used for iterator over Enum, including Phase and State. 
   */
  final static class Stage<E> {
    int currentIndex = 0;
    final E[] enums;

    /**
     * Adding all enum elements for iteration.
     * @param enums are all enum elements.
     */
    Stage(final E[] enums) {
      this.enums = enums;
    }

    /**
     * Return current element in the enum array and increase index by 1.
     * @return E is the enum type.
     */
    public E next() {
      final E e = enums[currentIndex];
      currentIndex += 1;
      return e;
    }

    /**
     * Reset index to the beginning of the enum.
     */
    public void reset() {
      currentIndex = 0;
    }

    /**
     * Check if reaching the end of the enum.
     * May call {@link #reset} for restart from the beginning.
     */
    public boolean isEnd() {
      return (enums.length == currentIndex);
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
    WAITING, RUNNING, SUCCEEDED, FAILED, STOPPED, CANCELLED 
  }


  public static final class Builder {

    private TaskAttemptID id;
    private HamaConfiguration conf = new HamaConfiguration();
    private long startTime = 0;
    private long finishTime = 0;
    private PartitionedSplit split = null;
    private int currentSuperstep = 1;
    private Phase phase = Phase.SETUP;
    private State state = State.WAITING;
    private boolean completed = false;
    private Marker marker = new Marker(false, ""); 

    public Builder setId(final TaskAttemptID id) {
      this.id = id;
      return this;
    }

    public Builder setConfiguration(final HamaConfiguration conf) {
      this.conf = conf;
      if(null == this.conf) 
        throw new IllegalArgumentException("HamaConfiguration not provided "+
                                           "when building the task.");
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

    /**
     * Split contains metadata pointed to the original dataset, including:
     * - split class.
     * - path of this split.
     * - hosts.
     * - partition id.
     * - data length.
     */
    public Builder setSplit(final PartitionedSplit split) {
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

    public Task build() {
      return new Task(id, 
                      conf,
                      startTime, 
                      finishTime, 
                      split, 
                      currentSuperstep,
                      state, 
                      phase,
                      completed, 
                      marker);
    }
  }

  public Task() {} // for Writable

  public Task(final TaskAttemptID id, 
              final HamaConfiguration conf,
              final long startTime, 
              final long finishTime, 
              final PartitionedSplit split, 
              final int currentSuperstep,
              final State state, 
              final Phase phase, 
              final boolean completed, 
              final Marker marker){
    this.id = id;
    if(null == this.id) 
      throw new IllegalArgumentException("TaskAttemptID not provided.");
    this.configuration = conf;
    if(null == this.configuration) 
      throw new IllegalArgumentException("HamaConfiguration is missing!");
    this.startTime = new LongWritable(startTime);
    this.finishTime = new LongWritable(finishTime);
    this.split = split;
    if(null == this.split)  
      LOG.warn("Split for task "+this.id.toString()+" is null. This might "+
               "indicate no input is required.");
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
  }

  public TaskAttemptID getId() {
    return this.id;
  }

  public HamaConfiguration getConfiguration() {
    return this.configuration;
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

  /**
   * Store split infomration, including:
   * - split class. 
   * - path of this split.
   * - a list of hosts.
   * - partition id.
   * - file length.
   * @return 
   */
  public PartitionedSplit getSplit() {
    return this.split;
  }

  public int getCurrentSuperstep() {
    return this.currentSuperstep.get();
  }
  
  /**
   * Increment the current superstep via 1.
   */
  public void increatmentSuperstep() {
    this.currentSuperstep = new IntWritable((getCurrentSuperstep()+1));
  }

  public State getState() {
    return this.state;
  }

  /**
   * Indiate this task is moved to the next state.
   */
  public void nextState() {
    if(!States.isEnd()) {
      this.state = States.next(); 
    } else {
      States.reset();
      this.state = States.next();
    }
  }

  public Phase getPhase() {
    return this.phase;
  }

  /**
   * Indiate this task is moved to the next phase.
   */
  public void nextPhase() {
    if(!Phases.isEnd()) {
      this.phase = Phases.next(); 
    } else {
      Phases.reset();
      this.phase = Phases.next();
    }
  }

  /**
   * Denote if this task is completed.
   * @return boolean denotes complete if true; otherwise false.
   */
  public boolean isCompleted() {
    return this.completed.get();
  }

  /**
   * Tell if this task is assigned to a particular {@link GroomServer}.
   */
  public boolean isAssigned() {
    return this.marker.isAssigned();
  }

  /**
   * Tell to which {@link GroomServer} this task is dispatched.
   * @return String of the target {@link GroomServer}.
   */
  public String getAssignedTarget() {
    return this.marker.getAssignedTarget();
  } 

  public void markWithTarget(final String name) {
    this.marker = new Marker(true, name);
  }

  /**
   * Total BSP tasks to be executed for a single job.
   * N.B.: This value should be read only once initialized.
   * @return int for the number of bsp tasks will be ran across the cluster.
   */
  public int getTotalBSPTasks() {
    return configuration.getInt("bsp.peers.num", 1);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    this.id.write(out);
    this.configuration.write(out);
    this.startTime.write(out);
    this.finishTime.write(out);
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
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.id = new TaskAttemptID();
    this.id.readFields(in);
    this.configuration = new HamaConfiguration();
    this.configuration.readFields(in);
    this.startTime = new LongWritable(0);
    this.startTime.readFields(in);
    this.finishTime = new LongWritable(0);
    this.finishTime.readFields(in);
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
  }

  @Override
  public boolean equals(Object o) {
    if (o == this)
      return true;
    if (null == o)
      return false;
    if (getClass() != o.getClass())
      return false;

    final Task s = (Task) o;
    if (!s.id.equals(id))
      return false;
    return true;
  }

  @Override
  public int hashCode() {
    int result = 17;
    result = 37 * result + id.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "Task("+id.toString() + "," +
                   getStartTime() + "," +
                   getFinishTime() + "," +
                   getState().toString() + "," +
                   getPhase().toString() + "," + 
                   isCompleted() + "," + 
                   marker.toString() + "," +
                   getTotalBSPTasks()+")";
  }
}

