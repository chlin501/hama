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
import org.apache.hama.SystemInfo;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.io.PartitionedSplit;

/**
 * A view to task information. 
 * Task building should be done through Task.Builder method. 
 * Builder constructor provides a way to create a new Task instance based from
 * old one provided.
 * This class can also produce metrics stats for monitor.
 */
public final class Task implements Writable { 

  final Log LOG = LogFactory.getLog(Task.class);

  /* The unique id for this task, including BSPJobID. */
  private TaskAttemptID id;

  /** 
   * This variable, derived from v2.Job, contains specific setting to this task.
   */
  private HamaConfiguration configuration = new HamaConfiguration();

  /* Time this task begins. */
  private LongWritable startTime = new LongWritable(0);

  /* Time this task finishes. */  
  private LongWritable finishTime = new LongWritable(0);

  /* The input meta data for this task. */
  private PartitionedSplit split;  

  // TODO: change current superstep type to long!
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
  private Marker marker = new Marker(false, SystemInfo.Localhost, 50000); 
 
  final static class Marker implements Writable {

    /* true when active schdule task to groom; default is false. */
    private BooleanWritable active = new BooleanWritable(false);

    /* true when this task is assigned; otherwise false. */
    private BooleanWritable assigned = new BooleanWritable(false);

    /* groom server host string. */
    private Text host = new Text();

    /* groom server port value. */
    private IntWritable port = new IntWritable(50000);

    /**
     * Target groom server can't be the same actor system of master, so 
     * port value is needed.
     */
    public Marker(final boolean isAssigned, final int portValue) {
      this(false, SystemInfo.Localhost, portValue);
    }
   
    public Marker(final boolean isAssigned, final String hostName, 
                  final int portValue) {
      this(false, isAssigned, hostName, portValue);
    }

    public Marker(final boolean isActive, final boolean isAssigned, 
                  final String hostName, final int portValue) {
      this.active = new BooleanWritable(isActive);
      this.assigned = new BooleanWritable(isAssigned); 
      if(null == hostName || "".equals(hostName)) {
        this.host = new Text(SystemInfo.Localhost);
      } else {
        this.host = new Text(hostName);
      }
      if(0 < portValue) {
        this.port = new IntWritable(portValue);
      } else throw new RuntimeException("Invalid groom port value: "+portValue);
    }

    /**
     * Denote if this task is scheduled in proactive.
     * @return boolean true if active; otherwise false.
     */
    public boolean isActive() {
      return active.get();
    }

    public boolean isAssigned() {
      return assigned.get();
    }

    /**
     * Tell to which GroomServer this task is dispatched.
     * @return String is the name of target GroomServer.
     */
    public String getAssignedHost() {
      return host.toString();
    }

    public int getAssignedPort() {
      return port.get();
    }

    @Override
    public void write(DataOutput out) throws IOException {
      active.write(out);
      assigned.write(out);
      Text.writeString(out, host.toString());
      port.write(out); 
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      active = new BooleanWritable(false);
      active.readFields(in);
      assigned = new BooleanWritable(false);
      assigned.readFields(in);
      host = new Text();
      host = new Text(Text.readString(in));
      port = new IntWritable(50000);
      port.readFields(in);
    }

    @Override
    public String toString() {
      return "Marker("+active.toString()+","+
                       assigned.toString()+","+
                       host.toString()+","+
                       port.get()+")";
    }
  }

  /**
   * Describe in which phase a task is in terms of supersteps.
   * The procedure is
   *             +-------------------------------------------------+
   *             |                                                 | 
   *             v                                                 | 
   * SETUP -> COMPUTE -> BARRIER_ENETER -> WITHIN_BARRIER -> BARRIER_LEAVE -> 
   * CLEANUP
   */
  public static enum Phase {
    SETUP, 
    COMPUTE, 
    BARRIER_ENTER, 
    WITHIN_BARRIER,
    BARRIER_LEAVE, 
    EXIT_BARRIER,
    CLEANUP 
  }

  /**
   * Indicate the current task state ie whether it's running, failed, etc. in a
   * particular executing point.
   */
  public static enum State {
    WAITING, RUNNING, SUCCEEDED, FAILED, CANCELLED 
  }

  public static final class Builder {

    private TaskAttemptID id;
    private HamaConfiguration conf = new HamaConfiguration();
    private long startTime;
    private long finishTime;
    private PartitionedSplit split = null;
    private int currentSuperstep;
    private Phase phase = Phase.SETUP;
    private State state = State.WAITING;
    private boolean completed = false;
    private Marker marker = new Marker(false, SystemInfo.Localhost, 50000); 

    public Builder() { }
 
    public Builder(final Task old) {
      if(null == old) 
        throw new IllegalArgumentException("Previous task not found!");
      this.id = old.getId();
      this.conf = new HamaConfiguration(old.getConfiguration());
      this.startTime = old.getStartTime();
      this.finishTime = old.getFinishTime();
      this.split = old.getSplit(); 
      this.currentSuperstep = old.getCurrentSuperstep(); 
      this.phase = old.getPhase();
      this.state = old.getState();
      this.completed = old.isCompleted(); 
      this.marker = new Marker(old.marker.isActive(), 
                               old.marker.isAssigned(),
                               old.marker.getAssignedHost(),
                               old.marker.getAssignedPort());  
    }

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
     * - path 
     * - partition id
     * - file start pos
     * - data length
     * - hosts
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

    /**
     * Mark this task is scheduled to a particular target groom.
     */
    public Builder scheduleTo(final String groom, final int port) {
      if(null == groom || groom.isEmpty()) 
        throw new IllegalArgumentException("Mark assigned, but groom server "+
                                           "name is not provided!");
      this.marker = new Marker(true, true, groom, port); 
      return this;
    }

    /**
     * Revoke schedule target from a particular GroomServer, and mark it as 
     * unassigned.
     */
    public Builder revoke() {
      this.marker = new Marker(false, SystemInfo.Localhost, 50000); 
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
      LOG.warn("No split for task "+this.id.toString()+". Perhaps no input "+
               "is required.");

    if(0 > currentSuperstep)
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
   * @return PartitionSplit instance.
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

  public void waitingState() {
    this.state = State.WAITING; 
  } 

  public void runningState() {
    this.state = State.RUNNING;
  }

  public void succeedState() {
    this.state = State.SUCCEEDED;
  }

  public void failedState() {
    this.state = State.FAILED;
  }

  public void cancelledState() {
    this.state = State.CANCELLED;
  }

  public Phase getPhase() {
    return this.phase;
  }

  public void setupPhase() {
    this.phase = Phase.SETUP;
  }

  public void computePhase() {
    this.phase = Phase.COMPUTE;
  }

  public void barrierEnterPhase() {
    this.phase = Phase.BARRIER_ENTER;
  }

  public void withinBarrierPhase() {
    this.phase = Phase.WITHIN_BARRIER;
  }

  public void barrierLeavePhase() {
    this.phase = Phase.BARRIER_LEAVE;
  }

  public void exitBarrierPhase() {
    this.phase = Phase.EXIT_BARRIER;
  }

  public void cleanupPhase() {
    this.phase = Phase.CLEANUP;
  }

  /**
   * Denote if this task is completed.
   * @return boolean denotes complete if true; otherwise false.
   */
  public boolean isCompleted() {
    return this.completed.get();
  }

  /**
   * Denote if this task is actively scheduled to a particular 
   * {@link GroomServer}.
   */
  public boolean isActive() {
    return this.marker.isActive();
  }  

  /**
   * Denote if this task is assigned to a particular {@link GroomServer}.
   */
  public boolean isAssigned() {
    return this.marker.isAssigned();
  }

  /**
   * Denote to which {@link GroomServer} this task is dispatched.
   * @return String of the target {@link GroomServer}.
   */
  public String getAssignedHost() {
    return this.marker.getAssignedHost();
  } 

  public int getAssignedPort() {
    return this.marker.getAssignedPort();
  }

  public String getAssignedHostPort() {
    return getAssignedHost() + ":" + getAssignedPort();
  }

  public SystemInfo runsAt() {
    final String sys = configuration.get("bsp.actor-system.name", "BSPSystem");
    return new SystemInfo(sys, getAssignedHost(), getAssignedPort()); 
  }

  /**
   * Mark this is an action that actively schedules a task to a particular 
   * target groom server.
   */
  public void scheduleTo(final String name, final int port) { 
    this.marker = new Marker(true, true, name, port); 
  } 

  public void revoke() { 
    this.marker = new Marker(false, SystemInfo.Localhost, 50000); 
  }

  /**
   * Mark this is an action that passively assigns a task to a particular groom
   * server which issuies a request.
   */
  public void assignedTo(final String name, final int port) { 
    this.marker = new Marker(false, true, name, port); 
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
      this.split = new PartitionedSplit();
      this.split.readFields(in);
    } else {
      this.split = null;
    } 
    this.currentSuperstep = new IntWritable(0);
    this.currentSuperstep.readFields(in);
    this.state = WritableUtils.readEnum(in, State.class);
    this.phase = WritableUtils.readEnum(in, Phase.class);
    this.completed = new BooleanWritable(false);
    this.completed.readFields(in);
    this.marker = new Marker(false, SystemInfo.Localhost, 50000); 
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

  /**
   * Create a new task with the same content.
   * @return Task is a new one.
   */
  public Task copyTask() {
    return new Builder(this).build();
  }

  public Task withIdIncremented() {
    return new Builder(this).setId(getId().next()).build();
  }

}

