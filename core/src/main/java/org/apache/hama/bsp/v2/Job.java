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
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPJobID;

/**
 * Read only informaion for a job.
 */
public final class Job implements Writable {

  /* Id for this job. */
  private BSPJobID id;

  /* name of this job. */
  private Text name;

  /* The user that owns this job. */
  private Text user;

  /* This is the job file or job.xml. */
  private Text xml; 

  /* job.xml stored in local fs. */
  private Text localJobFile;

  /* The jar file stored in local fs. */
  private Text localJarFile;

  /* The number of bsp tasks. */
  private IntWritable numBSPTasks = new IntWritable(1);

  /* The master server to which this job belongs. */
  private Text master = new Text("bspmaster");

  /* Max times a task can retry. */
  private IntWritable maxTaskAttempts = new IntWritable(3);

  /* Input dir where split files are stored. */
  private Text inputPath;

  /* Denote current job state. */
  private State state = State.PREP;

  /* % of bsp(). */
  private LongWritable progress = new LongWritable(0);

  /* % of setup(). */
  private LongWritable setupProgress = new LongWritable(0);

  /* % of cleanup(). */
  private LongWritable cleanupProgress = new LongWritable(0);

  /* Start time for this job. */
  private LongWritable startTime = new LongWritable(System.currentTimeMillis());

  /* Finish time for this job. */
  private LongWritable finishTime = new LongWritable(0);

  /* The i-th superstep. */
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
      String name = null;
      switch (this) {
        case PREP:
          name = "SETUP";
          break;  
        case RUNNING:
          name = "RUNNING";
          break;
        case SUCCEEDED:
          name = "SUCCEEDED";
          break;
        case FAILED:
          name = "FAILED";
          break;
        case CANCELLED:
          name = "CANCELLED";
          break;
      }
      return name;
    }
  }
  
  public static final class Builder {

    private BSPJobID id;
    private String name;
    private String user;
    private String xml; 
    private String localJobFile;
    private String localJarFile;
    private int numBSPTasks;
    private String master;
    private int maxTaskAttempts;
    private String inputPath;
    private State state;
    private long progress;
    private long setupProgress;
    private long cleanupProgress;
    private long startTime = System.currentTimeMillis();
    private long finishTime;
    private long superstepCount; 
    private HamaConfiguration conf = new HamaConfiguration();
    private TaskTable taskTable;
    private String[] targets = new String[]{};

    public Builder setId(final BSPJobID id) {
      this.id = id;
      return this;
    }

    public Builder setName(final String name) {
      this.name = name;
      return this;
    }

    public Builder setUser(final String user) {
      this.user = user;
      return this;
    }

    public Builder setJobXml(final String xml) { 
      this.xml = xml;
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

    public Builder setNumBSPTasks(final int numBSPTasks) {
      this.numBSPTasks = numBSPTasks;
      return this;
    }

    public Builder setMaster(final String master) {
      this.master = master;
      return this;
    }

    public Builder setMaxTaskAttempts(final int maxTaskAttempts) {
      this.maxTaskAttempts = maxTaskAttempts;
      return this;
    }

    public Builder setInputPath(final String inputPath) {
      this.inputPath = inputPath;
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
      return this;
    }

    public Builder setTaskTable(final TaskTable taskTable) {
      this.taskTable = taskTable;
      return this;
    }

    public Builder setTargets(final String[] targets) {
      this.targets = targets;
      return this;
    }

    public Job build() {
      return new Job(id, 
                     name, 
                     user, 
                     xml, 
                     localJobFile, 
                     localJarFile, 
                     numBSPTasks, 
                     master, 
                     maxTaskAttempts,
                     inputPath,
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
             final String name,
             final String user,
             final String xml, 
             final String localJobFile,
             final String localJarFile,
             final int numBSPTasks,
             final String master,
             final int maxTaskAttempts,
             final String inputPath,
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
    this.name = (null == name)? new Text(): new Text(name);
    this.conf = (null == conf)? new HamaConfiguration(): conf;
    this.user = (null == user)? new Text(): new Text(user);
    this.xml = (null == xml)? new Text(): new Text(xml);
    this.localJobFile = 
      (null == localJobFile)? new Text(): new Text(localJobFile);
    this.localJarFile = 
      (null == localJarFile)? new Text(): new Text(localJarFile);
    if(numBSPTasks > 0) {
      this.numBSPTasks = new IntWritable(numBSPTasks);
    } else {
      this.numBSPTasks = new IntWritable(conf.getInt("bsp.peers.num", 1));
    }
    this.master = (null == master)? new Text(): new Text(master);
    if(maxTaskAttempts > 0) {
      this.maxTaskAttempts = new IntWritable(maxTaskAttempts);
    } else {
      this.maxTaskAttempts = new IntWritable(
        conf.getInt("bsp.job.task.retry_n_times", 3)
      );
    }
    this.inputPath = (null == inputPath)? new Text(): new Text(inputPath);
    this.state = state;
    this.progress = new LongWritable(progress);
    this.setupProgress = new LongWritable(setupProgress);
    this.cleanupProgress = new LongWritable(cleanupProgress);
    this.startTime = new LongWritable(startTime);
    this.finishTime = new LongWritable(finishTime);
    this.superstepCount = new LongWritable(superstepCount);
    this.taskTable = taskTable;
    if(null == this.taskTable)
      throw new IllegalArgumentException("TaskTable is not presented.");

    if(null != targets && 0 < targets.length) {
      this.targets = new ArrayWritable(targets);
    }
  }

  public BSPJobID getId() {
    return this.id;
  }

  public String getName() {
    return this.name.toString();
  }

  public String getUser() {
    return this.user.toString();
  }

  public String getJobXml() {
    return this.xml.toString();
  }

  public String getLocalJobFile() {
    return this.localJobFile.toString();
  }

  public String getLocalJarFile() {
    return this.localJarFile.toString();
  }

  public int getNumBSPTasks() {
    return this.numBSPTasks.get();
  }

  public String getMaster() {
    return this.master.toString();
  }

  public int getMaxTaskAttempts() {
    return this.maxTaskAttempts.get();
  }

  public String getInputPath() {
    return this.inputPath.toString();
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
   */
  public String[] getTargets() {
    return this.targets.toStrings();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    id.write(out);
    name.write(out);
    user.write(out);
    xml.write(out); 
    localJobFile.write(out);
    localJarFile.write(out);
    numBSPTasks.write(out);
    master.write(out);
    maxTaskAttempts.write(out);
    inputPath.write(out);
    WritableUtils.writeEnum(out, state);
    progress.write(out);
    setupProgress.write(out);
    cleanupProgress.write(out);
    startTime.write(out);
    finishTime.write(out);
    superstepCount.write(out);
    conf.write(out);
    taskTable.write(out);
    targets.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.id = new BSPJobID();
    this.id.readFields(in);
    this.name = new Text();
    this.name.readFields(in);
    this.user = new Text();
    this.user.readFields(in);
    this.xml = new Text();
    this.xml.readFields(in); 
    this.localJobFile = new Text();
    this.localJobFile.readFields(in);
    this.localJarFile = new Text();
    this.localJarFile.readFields(in);
    this.numBSPTasks = new IntWritable();
    this.numBSPTasks.readFields(in);
    this.master = new Text();
    this.master.readFields(in);
    this.maxTaskAttempts = new IntWritable();
    this.maxTaskAttempts.readFields(in);
    this.inputPath = new Text();
    this.inputPath.readFields(in);
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
    this.taskTable = 
      new TaskTable(this.id, getNumBSPTasks(), getMaxTaskAttempts());
    this.taskTable.readFields(in);
    this.targets.readFields(in);
  }
}

