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
  private Text localJobFile = new Text("");

  /* The jar file path stored in local fs. */
  private Text localJarFile = new Text("");

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

  /* A list of GroomServers to which tasks will be scheduled. */
  private ArrayWritable targets = new ArrayWritable(new String[]{});

  // TODO: refactr? 
  public static enum State {
    PREP(1), RUNNING(2), SUCCEEDED(3), FAILED(4), CANCELLED(5);
    int s;

    State(int s) {
      this.s = s;
    }

    public int value() {
      return this.s;
    }

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
    private String localJobFile = "";
    private String localJarFile = "";
    private int lastCheckpoint;
    private State state = State.PREP;
    private long progress;
    private long setupProgress;
    private long cleanupProgress;
    private long startTime = System.currentTimeMillis();
    private long finishTime;
    private long superstepCount;  
    private TaskTable taskTable;
    private String[] targets = new String[]{};

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

    public Builder setLocalJobFile(final String localJobFile) {
      this.localJobFile = localJobFile;
      return this;
    }

    public Builder setLocalJarFile(final String localJarFile) {
      this.localJarFile = localJarFile;
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
      this.targets = targets;
      return this;
    }
     
    /**
     * This will create a temp array.
     */
    public Builder withTarget(final String groomServerName) {
      if(null == groomServerName || "".equals(groomServerName))
        throw new IllegalArgumentException("Provided groomServerName invalid.");
      final String[] ary = new String[this.targets.length+1];
      System.arraycopy(this.targets, 0, ary, 0, this.targets.length);
      ary[ary.length-1] = groomServerName;
      this.targets = ary;
      return this;
    }

    public Job build() {
      return new Job(id, 
                     localJobFile, 
                     localJarFile, 
                     lastCheckpoint, 
                     state,
                     progress,
                     setupProgress,
                     cleanupProgress,
                     startTime,
                     finishTime,
                     superstepCount,
                     conf,
                     taskTable, 
                     targets);
    }
  }

  Job() {} // for Writable

  public Job(final BSPJobID id,
             final String localJobFile,
             final String localJarFile,
             final int lastCheckpoint,
             final State state,
             final long progress,
             final long setupProgress,
             final long cleanupProgress,
             final long startTime,
             final long finishTime,
             final long superstepCount,
             final HamaConfiguration conf,
             final TaskTable taskTable, 
             final String[] targets) {
    this.id = id;
    if(null == this.id) 
      throw new IllegalArgumentException("BSPJobID is not provided.");
    this.conf = (null == conf)? new HamaConfiguration(): conf;
    this.localJobFile = 
      (null == localJobFile)? new Text(): new Text(localJobFile);
    this.localJarFile = 
      (null == localJarFile)? new Text(): new Text(localJarFile);

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

    if(null != targets && 0 < targets.length) {
      this.targets = new ArrayWritable(targets);
    }
    // verify targets length and numBSPTasks size
    final String[] targetServers = this.targets.toStrings();

    if(getNumBSPTasks() < targetServers.length) 
      throw new RuntimeException("Target value "+targetServers.length +
                                 " is larger than "+getNumBSPTasks() +
                                 " total tasks allowed.");
  }

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
    return this.localJobFile.toString();
  }

  public String getLocalJarFile() {
    return this.localJarFile.toString();
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

  public TaskTable getTasks() {
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
   */
  public String[] getTargets() {
    // TODO: group first?
    return this.targets.toStrings();
  }

  public boolean areAllTasksAssigned() {
    return this.taskTable.areAllTasksAssigned();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    id.write(out);
    localJobFile.write(out);
    localJarFile.write(out);
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
    targets.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.id = new BSPJobID();
    this.id.readFields(in);
    this.localJobFile = new Text();
    this.localJobFile.readFields(in);
    this.localJarFile = new Text();
    this.localJarFile.readFields(in);
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
    this.targets.readFields(in);
  }

  @Override 
  public String toString() {
    return "Job id: "+ id.toString()+ 
           " name: "+ getName() +
           " user: "+ getUser()+
           " localJobFile: "+localJobFile.toString()+
           " localJarFile: "+localJarFile.toString()+
           " lastCheckpoint: " +lastCheckpoint.toString()+
           " numBSPTasks: "+ getNumBSPTasks()+
           " master: "+getMaster()+
           " maxTaskAttempts: "+getMaxTaskAttempts()+
           " inputPath: "+getInputPath()+
           " outputPath: "+getOutputPath()+
           " state: "+ state.toString()+
           " progress: "+progress.toString()+
           " setupProgress: "+setupProgress.toString()+
           " cleanupProgress: "+cleanupProgress.toString()+
           " startTime: "+ startTime.toString()+
           " finishTime: "+finishTime.toString()+
           " superstepCount: " +superstepCount.toString()+
           " conf: "+conf.toString()+
           " taskTable: "+taskTable.toString()+
           " targets: "+targetsToString();  
  }

  private String targetsToString() {
    final StringBuilder sb = new StringBuilder();
    for(final String target: targets.toStrings()) {
      sb.append(target+" ");
    }
    return sb.toString();
  }
}

