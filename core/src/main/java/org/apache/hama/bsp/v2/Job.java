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

import akka.actor.ActorRef;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.SystemInfo;
import org.apache.hama.bsp.BSPJobID;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.io.PartitionedSplit;

/**
 * Job informaion data.
 */
public final class Job implements Writable {

  public static final Log LOG = LogFactory.getLog(Job.class);

  /* Id for this job. */
  private BSPJobID id;

/* The lastest superstep was successfully snapshotted. 
  private IntWritable lastCheckpoint = new IntWritable(0);  
*/

  /* Denote current job state. */
  private State state = State.PREP;

  /* % of bsp(). */
  private LongWritable progress = new LongWritable(0);

  /* % of setup(). */
  private LongWritable setupProgress = new LongWritable(0);

  /* % of cleanup(). */
  private LongWritable cleanupProgress = new LongWritable(0);

  /* Start time for this job. */
  private LongWritable startTime = new LongWritable(now());

  /* Finish time for this job. */
  private LongWritable finishTime = new LongWritable(0);

  /* The i-th superstep that is running right now. */
  private LongWritable superstepCount = new LongWritable(0); 

  /* Specific setting for this job. */
  private HamaConfiguration conf = new HamaConfiguration();

  /* 2d task array. size ~= numBSPTasks x maxTaskAttempts. */
  private TaskTable taskTable;

  public static enum State {
    PREP(1), 
    RUNNING(2), 
    SUCCEEDED(3), 
    /* issue cancel message. going to stop the job. */
    KILLING(4), // TODO: rename to stop?
    /* issue cancel message. going to resume all tasks. */
    RESTARTING(5), 
    FAILED(6);

    int s;

    State(int s) { this.s = s; }

    public int value() { return this.s; }

    @Override
    public String toString() {
      String stateName = null;
      switch (this) {
        case PREP:
          stateName = "PREP";
          break;  
        case RUNNING:
          stateName = "RUNNING";
          break;
        case SUCCEEDED:
          stateName = "SUCCEEDED";
          break;
        case KILLING:
          stateName = "KILLING";
          break;
        case RESTARTING:
          stateName = "RESTARTING";
          break;
        case FAILED:
          stateName = "FAILED";
          break;
      }
      return stateName;
    }
  }
  
  public static final class Builder {

    private BSPJobID id; 
    private HamaConfiguration conf = new HamaConfiguration(); 
/*
    private int lastCheckpoint;
*/
    private State state = State.PREP;
    private long progress;
    private long setupProgress;
    private long cleanupProgress;
    private long startTime = System.currentTimeMillis();
    private long finishTime;
    private long superstepCount;  
    private TaskTable taskTable;

    /**
     * This provides a way to create a default Job instance.
     */
    public Builder() { }

    /**
     * Create a new instance based on old Job instance's values.
     * User can override old values with related set methods.
     * @param old instance of a Job object.
     */
    public Builder(final Job old) {
      if(null == old) 
        throw new IllegalArgumentException("This constructor only accepts "+
                                           "creation based on old instance's "+
                                           "values.");
      this.id = old.getId();
      this.conf = old.getConfiguration(); 
/*
      this.lastCheckpoint = old.getLastCheckpoint();
*/
      this.state = old.getState();
      this.progress = old.getProgress();
      this.setupProgress = old.getSetupProgress();
      this.cleanupProgress = old.getCleanupProgress();
      this.startTime = old.getStartTime();
      this.finishTime = old.getFinishTime();
      this.superstepCount = old.getSuperstepCount();
      this.taskTable = old.getTasks();
    }

    public Builder setId(final BSPJobID id) { 
      if(null == this.id) this.id = id; else 
      LOG.warn("Job is created from old one so id "+this.id+" already exists!");
      return this;
    }

    public Builder setName(final String name) {
      if(null != name && !name.isEmpty()) conf.set("bsp.job.name", name);
      return this;
    }

    public Builder setUser(final String user) {
      if(null != user && !user.isEmpty()) conf.set("user.name", user);
      return this;
    }

    public Builder addLocalJobFile(final String localJobFilePath) {
      if(null != localJobFilePath && !"".equals(localJobFilePath))
        conf.addResource(new Path(localJobFilePath));
      return this;
    }

    public Builder setLocalJarFile(final String localJarFilePath) {
      if(null != localJarFilePath && !"".equals(localJarFilePath))
        conf.set("bsp.jar", localJarFilePath);
      return this;
    }

/*
    public Builder setLastCheckpoint(final int lastCheckpoint) {
      this.lastCheckpoint = lastCheckpoint;
      return this;
    }
*/

    public Builder setNumBSPTasks(final int numBSPTasks) {
      if(1 < numBSPTasks) conf.setInt("bsp.peers.num", numBSPTasks);
      adjustNumBSPTasks();
      return this;
    }

    public Builder setMaster(final String master) {
      if(null != master && !master.isEmpty())
        conf.set("master.name", master);
      return this;
    }

    public Builder setMaxTaskAttempts(final int maxTaskAttempts) {
      if(0 < maxTaskAttempts)
        conf.setInt("bsp.tasks.max.attempts", maxTaskAttempts);
      return this;
    }

    public Builder setInputPath(final String inputPath) {
      if(null != inputPath && !inputPath.isEmpty())
        conf.set("bsp.input.dir", inputPath);
      return this;
    }

    public Builder setOutputPath(final String outputPath) {
      if(null != outputPath && !outputPath.isEmpty())
        conf.set("bsp.output.dir", outputPath);
      return this;
    }

    public Builder setState(final State state) {
      this.state = state;
      return this;
    }

    public Builder setProgress(final long progress) {
      this.progress = progress;
      return this;
    }

    public Builder setSetupProgress(final long setupProgress) {
      this.setupProgress = setupProgress;
      return this;
    }

    public Builder setCleanupProgress(final long cleanupProgress) {
      this.cleanupProgress = cleanupProgress;
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

    public Builder setSuperstepCount(final long superstepCount) {
      this.superstepCount = superstepCount;
      return this;
    }

    public Builder setConf(final HamaConfiguration conf) {
      this.conf = conf;
      if(null == this.conf) 
        throw new IllegalArgumentException("No HamaConfiguration provided.");
      return this;
    }

    public Builder setTaskTable(final TaskTable taskTable) {
      if(null == this.taskTable)
        throw new IllegalArgumentException("TaskTable is missing."); 
      if(0 >= taskTable.rowLength())
        throw new IllegalArgumentException("Invalid TableTable numBSPTasks "+
                                           this.taskTable.rowLength());
      if(0 >= taskTable.getMaxTaskAttempts())
        throw new IllegalArgumentException("Invalid TableTable maxTaskAttempts"+
                                           this.taskTable.getMaxTaskAttempts());

      this.taskTable = taskTable;
      return this;
    }

    private void assertParameters() {
      if(null == this.id)
        throw new IllegalStateException("BSPJobID is missing when creating "+
                                        "TaskTable.");
    }
    
    private void adjustNumBSPTasks() {
      final String[] targets = conf.getStrings("bsp.target.grooms", 
                                               new String[]{});
      final int numbsptasks = conf.getInt("bsp.peers.num", 1);
      if(null != targets && targets.length > numbsptasks) { 
        LOG.info("Adjust numBSPTasks "+numbsptasks+" to "+targets.length+
                 " because target grooms to be scheduled is larger than "+
                 "num bsp tasks.");
        conf.setInt("bsp.peers.num", targets.length);  
      }
    }

    public Builder withTaskTable() {
      assertParameters();
      adjustNumBSPTasks();
      this.taskTable = new TaskTable(this.id, conf);
      return this;
    }

    public Builder withTaskTable(final PartitionedSplit[] splits) {
      assertParameters();
      adjustNumBSPTasks();
      if(null == splits) {
        return withTaskTable();
      } else {
        taskTable = new TaskTable(this.id, conf, splits);  
        return this;
      }
    }

    public Builder setTargets(final String[] targets) {
      if(null == targets || 0 == targets.length) 
        throw new IllegalArgumentException("Invalid targets string!");
      conf.setStrings("bsp.target.grooms", targets);
      return this;
    }
     
    /**
     * Add a target GroomServer on which the task may run.
     * This will create a temp array.
     * @param groomServerName is the target groom on which a task will run.
     * @return Builder contains necessary parameters.
     */
    public Builder withTarget(final String groomServerName) {
      if(null == groomServerName || "".equals(groomServerName))
        throw new IllegalArgumentException("Invalid GroomServerName "+
                                            groomServerName);
      final String[] newArray = 
         Arrays.copyOf(conf.getStrings("bsp.target.grooms"), 
                       conf.getStrings("bsp.target.grooms").length+1);
      newArray[newArray.length-1] = groomServerName;
      conf.setStrings("bsp.target.grooms", newArray);
      return this;
    }

    public Job build() {
      return new Job(id, 
/*
                     lastCheckpoint, 
*/
                     state,
                     progress,
                     setupProgress,
                     cleanupProgress,
                     startTime,
                     finishTime,
                     superstepCount,
                     conf,
                     taskTable);
    }

  }

  private static long now() {
    return System.currentTimeMillis();
  }

  Job() {} // for Writable

  public Job(final BSPJobID id,
/*
             final int lastCheckpoint,
*/
             final State state,
             final long progress,
             final long setupProgress,
             final long cleanupProgress,
             final long startTime,
             final long finishTime,
             final long superstepCount,
             final HamaConfiguration conf,
             final TaskTable taskTable) {
    this.id = id;
    if(null == this.id) 
      throw new IllegalArgumentException("BSPJobID is not provided.");
    this.conf = (null == conf)? new HamaConfiguration(): conf;

/*
    if(0 < lastCheckpoint) {
      this.lastCheckpoint = new IntWritable(lastCheckpoint);
    } else {
      this.lastCheckpoint = new IntWritable(0);
    }
*/

    this.state = state;
    if(null == this.state)
      throw new IllegalArgumentException("No initial State is assigned.");
    this.progress = new LongWritable(progress);
    this.setupProgress = new LongWritable(setupProgress);
    this.cleanupProgress = new LongWritable(cleanupProgress);
    this.startTime = new LongWritable(startTime);
    this.finishTime = new LongWritable(finishTime);
    if(0 > superstepCount) 
      throw new IllegalArgumentException("Invalid superstep "+superstepCount);
    this.superstepCount = new LongWritable(superstepCount);
    this.taskTable = taskTable;
    if(null == this.taskTable)
      throw new IllegalArgumentException("TaskTable is not presented.");

    // align numBSPTasks to # of splits calculated in TaskTable.
    final int actualNumBSPTasks = this.taskTable.getNumBSPTasks();
    conf.setInt("bsp.peers.num", actualNumBSPTasks); 
    LOG.info("Align numBSPTasks to "+actualNumBSPTasks);

    if(getNumBSPTasks() < targetGrooms().length) 
      throw new RuntimeException("Target GroomServer "+
                                 targetGrooms().length + " is larger than "+
                                 getNumBSPTasks() + " tasks allowed to run.");
  }

  /**
   * An unique identifier {@link BSPJobID} for this job.
   * @return BSPJobID that identifies a job.
   */
  public BSPJobID getId() {
    return this.id;
  }

  /**
   * Get BSP job name; default to empty string.
   * @return String is name of this job.
   */
  public String getName() {
    return conf.get("bsp.job.name", "");
  }

  /**
   * Get job user; default set to system property "user.name".
   * @return String is the user that owns this job.
   */
  public String getUser() {
    return conf.get("user.name", System.getProperty("user.name"));
  }

  public String getLocalJarFile() {
    return conf.get("bsp.jar");
  }

/*
  public int getLastCheckpoint() {
    return this.lastCheckpoint.get();
  }

  public Job newWithLastCheckpoint(final int lastCheckpoint) {
    return new Builder(this).setLastCheckpoint(lastCheckpoint).build(); 
  }
*/

  /**
   * Get the number of BSP tasks to run; default set to 1.
   * @return int denotes the tasks will be executed.
   */
  public int getNumBSPTasks() {
    return conf.getInt("bsp.peers.num", 1);
  }

  /**
   * Denote on which master this job runs; default to bspmaster.
   * @return String of the master name.
   */
  public String getMaster() {
    return conf.get("master.name", "bspmaster");
  }

  /**
   * Obtain max task attempts if a task fails; default to 3.
   * @return int for retry of a task can attempt.
   */
  public int getMaxTaskAttempts() {
    return conf.getInt("bsp.tasks.max.attempts", 3);
  }

  /**
   * Obtain inputPath directory; default to null.
   * @return String of input direactory.
   */
  public String getInputPath() {
    return conf.get("bsp.input.dir");
  }

  /**
   * Obtain outputPath directory; default to null.
   * @return String of output directory.
   */
  public String getOutputPath() {
    return conf.get("bsp.output.dir");
  }

  public State getState() {
    return this.state;
  }
  
  public Job newWithState(final State newState) {
    return new Builder(this).setState(newState).build();
  }

  public boolean isPrep() {
    return State.PREP.equals(getState());
  }

  public Job newWithPrepState() { 
    return newWithState(State.PREP); 
  }

  public boolean isRunning() {
    return State.RUNNING.equals(getState());
  }

  public Job newWithRunningState() { 
    return newWithState(State.RUNNING); 
  }

  public boolean isSucceeded() {
    return State.SUCCEEDED.equals(getState());
  }

  public Job newWithSucceededState() { 
    return newWithState(State.SUCCEEDED); 
  }

  public boolean isKilling() {
    return State.KILLING.equals(getState());
  }

  public Job newWithKillingState() { 
    return newWithState(State.KILLING); 
  }

  public boolean isRestarting() {
    return State.RESTARTING.equals(getState());
  }

  public Job newWithRestartingState() { 
    return newWithState(State.RESTARTING); 
  }

  public boolean isRecovering() {
    return (State.KILLING.equals(getState()) || 
           State.RESTARTING.equals(getState()));
  }

  public boolean isFailed() {
    return State.FAILED.equals(getState());
  }

  public Job newWithFailedState() {
    return newWithState(State.FAILED); 
  }

  public long getProgress() {
    return this.progress.get();
  }

  public Job newWithProgress(final long progress) {
    return new Builder(this).setProgress(progress).build();
  }

  public long getSetupProgress() {
    return this.setupProgress.get();
  }

  public Job newWithSetupProgress(final long setupProgress) {
    return new Builder(this).setSetupProgress(setupProgress).build();
  }

  public long getCleanupProgress() {
    return this.cleanupProgress.get();
  }

  public Job newWithCleanupProgress(final long cleanupProgress) {
    return new Builder(this).setCleanupProgress(cleanupProgress).build();
  }

  public long getStartTime() {
    return this.startTime.get();
  }

  public Job newWithStartTime(final long startTime) {
    return new Builder(this).setStartTime(startTime).build();
  }

  public long getFinishTime() {
    return this.finishTime.get();
  }

  public Job newWithFinishTime(final long finishTime) {
    return new Builder(this).setFinishTime(finishTime).build();
  }

  public long getSuperstepCount() {
    return this.superstepCount.get();
  }

  public Job newWithSuperstepCount(final long superstepCount) {
    return new Builder(this).setSuperstepCount(superstepCount).build();
  }

  public HamaConfiguration getConfiguration() {
    return this.conf;
  }

  public Job newWithConfiguation(final HamaConfiguration conf) {
    return new Builder(this).setConf(conf).build(); 
  }

  protected TaskTable getTasks() { 
    return this.taskTable;
  }

  public boolean markCancelledWith(final String taskAttemptId) {
    return getTasks().markCancelledWith(TaskAttemptID.forName(taskAttemptId));
  }

  public boolean markCancelledWith(final TaskAttemptID taskAttemptId) {
    return getTasks().markCancelledWith(taskAttemptId); 
  }

  public boolean allTasksStopped() { 
    return getTasks().allTasksStopped();
  } 

  public boolean allTasksSucceeded() {
    return getTasks().allTasksSucceeded();
  }
  
  public boolean allTasksAssigned() {
    return getTasks().allTasksAssigned();
  }

  public Task findTaskBy(final TaskAttemptID taskAttemptId) {
    return getTasks().findTaskBy(taskAttemptId);
  }

  public List<Task> findTasksBy(final String host, final int port) {
    return getTasks().findTasksBy(host, port);
  }

  public List<Task> findTasksNotIn(final String host, final int port) {
    return getTasks().findTasksNotIn(host, port);
  }

  /**
   * Tasktable:  
   * - Create a new task, with attempt id incremented, based on the old one.
   * - Reset task to waiting state.
   * - Revoke host port value from previously scheduled groom server.
   * - Append the new task to the end of corresponded column.
   * - Mark the old task as failure.
  public Task rearrange(final Task old) {
    final Task newTask = old.withIdIncremented(); 
    newTask.waitingState();
    newTask.revoke();
    getTasks().add(newTask);
    old.failedState();
    return newTask;
  }
   */

  /**
   * This function is mainly used for checking next task available. 
   * @return Task contains the latest task attempt id for those share the same
   *              task id.
   */
  public Task nextUnassignedTask() {
    return getTasks().nextUnassignedTask();
  }

  // TODO: return Progress object, instead of boolean.
  public boolean update(final Task newest) {
    return getTasks().update(newest);
  }

  /**
   * Return an array of GroomServers containing host and port values.
   * If it's array size is 0, indicating all tasks are passive assigning to 
   * GroomServers.
   * Note: target servers are not distincted/ grouped here because multiple 
   *       tasks may run on the same GroomServer.
   */
  public String[] targetGrooms() {
    return conf.getStrings("bsp.target.grooms", new String[]{});
  }

  /**
   * Obtain target grooms in the form of string.
   * @return String of all target GroomServers, delimited by comma(,).
   */
  String getTargetStrings() {
    return conf.get("bsp.target.grooms", "");
  }

  public SystemInfo[] targetInfos() {
    final String sys = conf.get("bsp.actor-system.name", "BSPSystem");
    final String[] targets = targetGrooms();
    SystemInfo[] infos = new SystemInfo[0];
    if(null != targets && 0 != targets.length) {
      infos = new SystemInfo[targets.length];
      for(int idx= 0; idx < targets.length; idx++) {
        final String[] hostPort = targets[idx].split(":");
        if(null == hostPort || 2 != hostPort.length) 
          throw new RuntimeException("Invalid host port "+targets[idx]+"!");
        final String host = hostPort[0];
        final String port = hostPort[1];
        try {
          final int p = Integer.parseInt(port);
          final SystemInfo info = new SystemInfo(sys, host, p);
          infos[idx] = info;
        } catch(NumberFormatException nfe) {
          throw new RuntimeException("Invalid port value: "+port, nfe);
        }
      }
    } 
    return infos;
  }

  /**
   * Return groom servers on which tasks are running. 
   * Multiple tasks may run on the same groom server, but only one info will be
   * returned by this function. 
   */
  public Set<SystemInfo> tasksRunAt() {
    return getTasks().grooms(); 
  }

  /**
   * Return groom servers on which tasks are running, except the failed one. 
   * Multiple tasks may run on the same groom server, but only one info will be
   * returned by this function. 
   * Note that failed task may run on the same servers as tasks still alive, so
   * grooms returned may contain that hosts failed task.
   */  
  public Set<SystemInfo> tasksRunAtExcept(final Task failed) {
    return getTasks().groomsExcept(failed); 
  }

  /**
   * Find the current task assignment count according to the GroomServer name.
   * @param groomServerName is the group key to tasks assigned to it.
   * @return int is the count of tasks assigned to the same GroomServer.
   */
  public int getTaskCountFor(final String groomServerName) {
    final Integer count = getTasks().group().get(groomServerName);
    int cnt = 0;
    if(null != count) cnt = count.intValue();
    return cnt;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    id.write(out);
/*
    lastCheckpoint.write(out);
*/
    WritableUtils.writeEnum(out, state);
    progress.write(out);
    setupProgress.write(out);
    cleanupProgress.write(out);
    startTime.write(out);
    finishTime.write(out);
    superstepCount.write(out);
    conf.write(out);
    if(null != getTasks()) {
      out.writeBoolean(true);
      taskTable.write(out);
    } else {
      out.writeBoolean(false);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.id = new BSPJobID();
    this.id.readFields(in);
/*
    this.lastCheckpoint = new IntWritable(0);
    this.lastCheckpoint.readFields(in);
*/
    this.state = WritableUtils.readEnum(in, State.class);
    this.progress = new LongWritable();
    this.progress.readFields(in);
    this.setupProgress = new LongWritable();
    this.setupProgress.readFields(in);
    this.cleanupProgress = new LongWritable();
    this.cleanupProgress.readFields(in);
    this.startTime = new LongWritable();
    this.startTime.readFields(in);
    this.finishTime = new LongWritable();
    this.finishTime.readFields(in);
    this.superstepCount = new LongWritable();
    this.superstepCount.readFields(in);
    this.conf = new HamaConfiguration();
    this.conf.readFields(in);
    if(in.readBoolean()) {
      this.taskTable = new TaskTable();
      this.taskTable.readFields(in);
    } else {
      LOG.warn("TaskTable for id "+this.id.toString()+" is null!");
      this.taskTable = null;
    } 
  }

  @Override
  public boolean equals(Object o) {
    if (o == this)
      return true;
    if (null == o)
      return false;
    if (getClass() != o.getClass())
      return false;

    final Job s = (Job) o;
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
    return "Job("+ id.toString()+ "," +
                   getName() + "," +
                   getUser() + "," + 
/*
                   lastCheckpoint.toString() + "," +
*/
                   getNumBSPTasks() + "," +
                   getMaster() + "," +
                   getMaxTaskAttempts() + "," +
                   getInputPath() + "," +
                   getOutputPath() + "," +
                   state.toString() + "," +
                   progress.toString() + "," +
                   setupProgress.toString() + "," +
                   cleanupProgress.toString() + "," +
                   startTime.toString() + "," +
                   finishTime.toString() + "," +
                   superstepCount.toString() + "," +
                   conf.toString() + "," +
                   taskTable.toString() + "," +
                   getTargetStrings() +")";  
  }

}

