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
import java.util.Arrays;
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
import org.apache.hama.bsp.BSPJobClient;
import org.apache.hama.bsp.BSPJobClient.RawSplit;
import org.apache.hama.bsp.BSPJobID;

/**
 * Read only informaion for a job.
 */
public final class Job implements Writable {

  public static final Log LOG = LogFactory.getLog(Job.class);

  private long _current = System.currentTimeMillis();

  /* Id for this job. */
  private BSPJobID id;

  /* job.xml path stored in local fs. */
  private Text localJobFilePath = new Text("");

  /* The jar file path stored in local fs. */
  private Text localJarFilePath = new Text("");

  /* The lastest superstep was successfully snapshotted. */
  private IntWritable lastCheckpoint = new IntWritable(0);  // only a record.

  /* Denote current job state. */
  private State state = State.PREP;

  /* % of bsp(). */
  private LongWritable progress = new LongWritable(0);

  /* % of setup(). */
  private LongWritable setupProgress = new LongWritable(0);

  /* % of cleanup(). */
  private LongWritable cleanupProgress = new LongWritable(0);

  /* Start time for this job. */
  private LongWritable startTime = new LongWritable(_current);

  /* Finish time for this job. */
  private LongWritable finishTime = new LongWritable(0);

  /* The i-th superstep that is running right now. */
  private LongWritable superstepCount = new LongWritable(0); 

  /* Specific setting for this job. */
  private HamaConfiguration conf = new HamaConfiguration();

  /* 2d task array. size ~= numBSPTasks x maxTaskAttempts. */
  private TaskTable taskTable;

  public static enum State {
    PREP(1), RUNNING(2), SUCCEEDED(3), FAILED(4), CANCELLED(5);
    int s;

    State(int s) { this.s = s; }

    public int value() { return this.s; }

    @Override
    public String toString() {
      String stateName = null;
      switch (this) {
        case PREP:
          stateName = "SETUP";
          break;  
        case RUNNING:
          stateName = "RUNNING";
          break;
        case SUCCEEDED:
          stateName = "SUCCEEDED";
          break;
        case FAILED:
          stateName = "FAILED";
          break;
        case CANCELLED:
          stateName = "CANCELLED";
          break;
      }
      return stateName;
    }
  }
  
  public static final class Builder {

    private BSPJobID id;
    private HamaConfiguration conf = new HamaConfiguration();
    private String localJobFilePath = "";
    private String localJarFilePath = "";
    private int lastCheckpoint;
    private State state = State.PREP;
    private long progress;
    private long setupProgress;
    private long cleanupProgress;
    private long startTime = System.currentTimeMillis();
    private long finishTime;
    private long superstepCount;  
    private TaskTable taskTable;

    public Builder setId(final BSPJobID id) {
      this.id = id;
      return this;
    }

    public Builder setName(final String name) {
      if(null != name && !name.isEmpty())
        conf.set("bsp.job.name", name);
      return this;
    }

    public Builder setUser(final String user) {
      if(null != user && !user.isEmpty())
        conf.set("user.name", user);
      return this;
    }

    public Builder setLocalJobFile(final String localJobFilePath) {
      this.localJobFilePath = localJobFilePath;
      return this;
    }

    public Builder setLocalJarFile(final String localJarFilePath) {
      this.localJarFilePath = localJarFilePath;
      return this;
    }

    public Builder setLastCheckpoint(final int lastCheckpoint) {
      this.lastCheckpoint = lastCheckpoint;
      return this;
    }

    public Builder setNumBSPTasks(final int numBSPTasks) {
      if(1 < numBSPTasks)
        conf.setInt("bsp.peers.num", numBSPTasks);
      return this;
    }

    public Builder setMaster(final String master) {
      if(null != master && !master.isEmpty())
        conf.set("bsp.master.name", master);
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
      if(0 >= taskTable.columnLength())
        throw new IllegalArgumentException("Invalid TableTable maxTaskAttempts"+
                                           this.taskTable.columnLength());

      this.taskTable = taskTable;
      return this;
    }

    private void assertParameters() {
      if(null == this.id)
        throw new IllegalStateException("BSPJobID is missing when creating "+
                                        "TaskTable.");
    }

    public Builder withTaskTable() {
      assertParameters();
      this.taskTable = new TaskTable(this.id, 
                                     conf.getInt("bsp.peers.num", 1), 
                                     conf.getInt("bsp.tasks.max.attempts", 3), 
                                     null);
      return this;
    }

    public Builder withTaskTable(final BSPJobClient.RawSplit[] splits) {
      assertParameters();
      if(null == splits) {
        return withTaskTable();
      } else {
        taskTable = new TaskTable(this.id, 
                                  conf.getInt("bsp.peers.num", 1), 
                                  conf.getInt("bsp.tasks.max.attempts", 3), 
                                  splits);  
        return this;
      }
    }

    public Builder setTargets(final String[] targets) {
      if(null == targets || 0 == targets.length) 
        throw new IllegalArgumentException("Invalid targets provided");
      conf.setStrings("bsp.sched.targets.grooms", targets);
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
         Arrays.copyOf(conf.getStrings("bsp.sched.targets.grooms"), 
                       conf.getStrings("bsp.sched.targets.grooms").length+1);
      newArray[newArray.length-1] = groomServerName;
      conf.setStrings("bsp.sched.targets.grooms", newArray);
      return this;
    }

    public Job build() {
      return new Job(id, 
                     localJobFilePath, 
                     localJarFilePath, 
                     lastCheckpoint, 
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

  Job() {} // for Writable

  public Job(final BSPJobID id,
             final String localJobFilePath,
             final String localJarFilePath,
             final int lastCheckpoint,
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
    this.localJobFilePath = 
      (null == localJobFilePath)? new Text(""): new Text(localJobFilePath);
    this.localJarFilePath = 
      (null == localJarFilePath)? new Text(""): new Text(localJarFilePath);

    if(0 < lastCheckpoint) {
      this.lastCheckpoint = new IntWritable(lastCheckpoint);
    } else {
      this.lastCheckpoint = new IntWritable(0);
    }

    this.state = state;
    if(null == this.state)
      throw new IllegalArgumentException("No initial State is assigned.");
    this.progress = new LongWritable(progress);
    this.setupProgress = new LongWritable(setupProgress);
    this.cleanupProgress = new LongWritable(cleanupProgress);
    this.startTime = new LongWritable(startTime);
    this.finishTime = new LongWritable(finishTime);
    this.superstepCount = new LongWritable(superstepCount);
    this.taskTable = taskTable;
    if(null == this.taskTable)
      throw new IllegalArgumentException("TaskTable is not presented.");

    // align numBSPTasks to # of splits calculated in TaskTable.
    final int actualNumBSPTasks = this.taskTable.getNumBSPTasks();
    LOG.info("Adjust numBSPTasks to "+actualNumBSPTasks);
    conf.setInt("bsp.peers.num", actualNumBSPTasks); 

    if(getNumBSPTasks() < getTargets().length) 
      throw new RuntimeException("Target value "+getTargets().length +
                                 " is larger than "+getNumBSPTasks() +
                                 " total tasks allowed.");
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

  public String getLocalJobFile() {
    return this.localJobFilePath.toString();
  }

  public String getLocalJarFile() {
    return this.localJarFilePath.toString();
  }

  public int getLastCheckpoint() {
    return this.lastCheckpoint.get();
  }

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
    return conf.get("bsp.master.name", "bspmaster");
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

  public long getProgress() {
    return this.progress.get();
  }

  public long getSetupProgress() {
    return this.setupProgress.get();
  }

  public long getCleanupProgress() {
    return this.cleanupProgress.get();
  }

  public long getStartTime() {
    return this.startTime.get();
  }

  public long getFinishTime() {
    return this.finishTime.get();
  }

  public long getSuperstepCount() {
    return this.superstepCount.get();
  }

  public HamaConfiguration getConfiguration() {
    return this.conf;
  }

  public TaskTable getTasks() { // TODO: consider to hide taskTable ref.
    return this.taskTable;
  }

  /**
   * This function is mainly used by Scheduler for checking next available 
   * task. 
   */
  public Task nextUnassignedTask() {
    return this.taskTable.nextUnassignedTask();
  }

  /**
   * Return an array of GroomServers name.
   * If it's array size is 0, indicating all tasks are passive assigning to 
   * GroomServers.
   * Note: target servers are not distincted/ grouped here because multiple 
   *       tasks may run on the same GroomServer.
   */
  public String[] getTargets() {
    return conf.getStrings("bsp.sched.targets.grooms", new String[]{});
  }

  /**
   * Obtain tasks in a form of string.
   * @return String of all target GroomServers, delimited by comma(,).
   */
  String getTargetStrings() {
    return conf.get("bsp.sched.targets.grooms", "");
  }

  public boolean areAllTasksAssigned() {
    return this.taskTable.areAllTasksAssigned();
  }

  /**
   * Find the current task assignment count according to the GroomServer name.
   * @param groomServerName is the group key to tasks assigned to it.
   * @return int is the count of tasks assigned to the same GroomServer.
   */
  public int getTaskCountFor(final String groomServerName) {
    final Integer count = this.taskTable.group().get(groomServerName);
    int cnt = 0;
    if(null != count) cnt = count.intValue();
    return cnt;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    id.write(out);
    localJobFilePath.write(out);
    localJarFilePath.write(out);
    lastCheckpoint.write(out);
    WritableUtils.writeEnum(out, state);
    progress.write(out);
    setupProgress.write(out);
    cleanupProgress.write(out);
    startTime.write(out);
    finishTime.write(out);
    superstepCount.write(out);
    conf.write(out);
    if(null != this.taskTable) {
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
    this.localJobFilePath = new Text("");
    this.localJobFilePath.readFields(in);
    this.localJarFilePath = new Text("");
    this.localJarFilePath.readFields(in);
    this.lastCheckpoint = new IntWritable(0);
    this.lastCheckpoint.readFields(in);
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
                   localJobFilePath.toString() + "," +
                   localJarFilePath.toString() + "," +
                   lastCheckpoint.toString() + "," +
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

