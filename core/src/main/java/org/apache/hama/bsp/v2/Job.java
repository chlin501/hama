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

  /* name of this job. */
  private Text name = new Text("");

  /* The user that owns this job. */
  private Text user = new Text("");

  /* job.xml path stored in local fs. */
  private Text localJobFile = new Text("");

  /* The jar file path stored in local fs. */
  private Text localJarFile = new Text("");

  /* The lastest superstep was successfully snapshotted. */
  private IntWritable lastCheckpoint = new IntWritable(0);

  /* The number of bsp tasks. */
  private IntWritable numBSPTasks = new IntWritable(1);

  /* The master server to which this job belongs. */
  private Text master = new Text("bspmaster");

  /* Max times a task can retry. */
  private IntWritable maxTaskAttempts = new IntWritable(3);

  /* Input dir where split files are stored. */
  private Text inputPath = new Text("");

  /* Output dir where final data to be stored. */
  private Text outputPath = new Text("");

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
    private String name = "";
    private String user = "";
    //private String xml = ""; 
    private String localJobFile = "";
    private String localJarFile = "";
    private int numBSPTasks = 1;
    private int lastCheckpoint;
    private String master = "";
    private int maxTaskAttempts = 3;
    private String inputPath = "";
    private String outputPath = "";
    private State state = State.PREP;
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

    public Builder setOutputPath(final String outputPath) {
      this.outputPath = outputPath;
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

    /**
     * This function will also
     * - setNumBSPTask 
     * - setMaxTaskAttempts
     * - setUser
     * - setName
     * from HamaConfiguration file. So if conf file doesn't contain related
     * info provided for those set methods. Information will be overriden.
     */
    public Builder setConf(final HamaConfiguration conf) {
      this.conf = conf;
      if(null == this.conf) 
        throw new IllegalArgumentException("Configuration provided is null.");
      setNumBSPTasks(this.conf.getInt("bsp.peers.num", 1));
      setMaxTaskAttempts(this.conf.getInt("bsp.tasks.max.attempts", 3));
      setUser(this.conf.get("user.name", System.getProperty("user.name")));
      setName(this.conf.get("bsp.job.name", ""));
      setInputPath(this.conf.get("bsp.input.dir", ""));
      return this;
    }

    public Builder setTaskTable(final TaskTable taskTable) {
      if(null == this.taskTable)
        throw new IllegalArgumentException("TaskTable is missing."); 
      if(0 >= this.taskTable.rowLength())
        throw new IllegalArgumentException("Invalid TableTable numBSPTasks "+
                                           this.taskTable.rowLength());
      if(0 >= this.taskTable.columnLength())
        throw new IllegalArgumentException("Invalid TableTable maxTaskAttempts"+
                                           this.taskTable.columnLength());

      this.taskTable = taskTable;
      return this;
    }

    private void assertParameters() {
      if(null == this.id)
        throw new IllegalStateException("BSPJobID is missing when creating "+
                                        "TaskTable.");
      if(0 >= numBSPTasks)
        throw new IllegalStateException("numBSPTasks is missing for creating "+
                                         "TaskTable.");
      if(0 >= maxTaskAttempts)
        throw new IllegalStateException("maxTaskAttempts is missing when "+
                                        "creating TaskTable.");
    }

    public Builder withTaskTable() {
      assertParameters();
      this.taskTable = new TaskTable(this.id, 
                                     numBSPTasks, 
                                     maxTaskAttempts, 
                                     null);
      return this;
    }

    public Builder withTaskTable(final BSPJobClient.RawSplit[] splits) {
      assertParameters();
      if(null == splits) {
        return withTaskTable();
      } else {
        this.taskTable = new TaskTable(this.id, 
                                       numBSPTasks, 
                                       maxTaskAttempts,
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
                     name, 
                     user, 
                     localJobFile, 
                     localJarFile, 
                     lastCheckpoint, 
                     numBSPTasks, 
                     master, 
                     maxTaskAttempts,
                     inputPath,
                     outputPath,
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
             final String localJobFile,
             final String localJarFile,
             final int lastCheckpoint,
             final int numBSPTasks,
             final String master,
             final int maxTaskAttempts,
             final String inputPath,
             final String outputPath,
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
    this.localJobFile = 
      (null == localJobFile)? new Text(): new Text(localJobFile);
    this.localJarFile = 
      (null == localJarFile)? new Text(): new Text(localJarFile);

    if(0 < lastCheckpoint) {
      this.lastCheckpoint = new IntWritable(lastCheckpoint);
    } else {
      this.lastCheckpoint = new IntWritable(0);
    }

    if(0 < numBSPTasks) {
      this.numBSPTasks = new IntWritable(numBSPTasks);
    } else {
      this.numBSPTasks = new IntWritable(conf.getInt("bsp.peers.num", 1));
    }

    this.master = (null == master)? new Text(): new Text(master);
    if(0 < maxTaskAttempts) {
      this.maxTaskAttempts = new IntWritable(maxTaskAttempts);
    } else {
      this.maxTaskAttempts = new IntWritable(
        conf.getInt("bsp.job.task.retry_n_times", 3)
      );
    }
    this.inputPath = (null == inputPath)? new Text(""): new Text(inputPath);
    this.outputPath = (null == outputPath)? new Text(""): new Text(outputPath);
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

    // align numBSPTasks to #splits
    final int actualNumBSPTasks = this.taskTable.getNumBSPTasks();
    this.numBSPTasks = new IntWritable(actualNumBSPTasks); 
    conf.setInt("bsp.peers.num", actualNumBSPTasks);

    if(null != targets && 0 < targets.length) {
      this.targets = new ArrayWritable(targets);
    }
    // verify targets length and numBSPTasks size
    final String[] tmp = this.targets.toStrings();

    if(numBSPTasks < tmp.length) 
      throw new RuntimeException("Target value "+tmp.length +" is larger "+
                                 "than "+numBSPTasks +" total tasks allowed.");
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

  public String getLocalJobFile() {
    return this.localJobFile.toString();
  }

  public String getLocalJarFile() {
    return this.localJarFile.toString();
  }

  public int getLastCheckpoint() {
    return this.lastCheckpoint.get();
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

  public String getOutputPath() {
    return this.outputPath.toString();
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
    return this.targets.toStrings();
  }

  public boolean areAllTasksAssigned() {
    return this.taskTable.areAllTasksAssigned();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    id.write(out);
    name.write(out);
    user.write(out);
    localJobFile.write(out);
    localJarFile.write(out);
    lastCheckpoint.write(out);
    numBSPTasks.write(out);
    master.write(out);
    maxTaskAttempts.write(out);
    inputPath.write(out);
    outputPath.write(out);
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
    this.name = new Text();
    this.name.readFields(in);
    this.user = new Text();
    this.user.readFields(in);
    this.localJobFile = new Text();
    this.localJobFile.readFields(in);
    this.localJarFile = new Text();
    this.localJarFile.readFields(in);
    this.lastCheckpoint = new IntWritable(0);
    this.lastCheckpoint.readFields(in);
    this.numBSPTasks = new IntWritable(1);
    this.numBSPTasks.readFields(in);
    this.master = new Text();
    this.master.readFields(in);
    this.maxTaskAttempts = new IntWritable(3);
    this.maxTaskAttempts.readFields(in);
    this.inputPath = new Text("");
    this.inputPath.readFields(in);
    this.outputPath = new Text("");
    this.outputPath.readFields(in);
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
           " name: "+name.toString()+
           " user: "+ user.toString()+
           " localJobFile: "+localJobFile.toString()+
           " localJarFile: "+localJarFile.toString()+
           " lastCheckpoint: " +lastCheckpoint.toString()+
           " numBSPTasks: "+ numBSPTasks.toString()+
           " master: "+master.toString()+
           " maxTaskAttempts: "+maxTaskAttempts.toString()+
           " inputPath: "+inputPath.toString()+
           " outputPath: "+outputPath.toString()+
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

