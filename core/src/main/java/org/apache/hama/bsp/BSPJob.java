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
package org.apache.hama.bsp;

import java.io.IOException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Enumeration;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.Constants;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.message.compress.BSPMessageCompressor;
import org.apache.hama.bsp.message.compress.BSPMessageCompressorFactory;
import org.apache.hama.bsp.message.queue.MessageQueue;

/**
 * A BSP job configuration.
 * 
 * BSPJob is the primary interface for a user to describe a BSP job to the Hama
 * BSP framework for execution.
 */
public class BSPJob extends BSPJobContext { // TODO: builder, immutable
  public static enum JobState {
    DEFINE, RUNNING
  }

  private JobState state = JobState.DEFINE;
  private BSPJobClient jobClient;
  private RunningJob info;

  public BSPJob() throws IOException {
    this(new HamaConfiguration());
  }

  public BSPJob(HamaConfiguration conf) throws IOException {
    super(conf, null);
    jobClient = new BSPJobClient(conf);
  }

  public BSPJob(HamaConfiguration conf, BSPJobID jobID) throws IOException {
    super(conf, jobID);
  }

  public BSPJob(HamaConfiguration conf, String jobName) throws IOException {
    this(conf);
    setJobName(jobName);
  }

  public BSPJob(BSPJobID jobID, String jobFile) throws IOException {
    super(new Path(jobFile), jobID);
  }

  public BSPJob(HamaConfiguration conf, Class<?> exampleClass)
      throws IOException {
    this(conf);
    setJarByClass(exampleClass);
  }

  public BSPJob(HamaConfiguration conf, int numPeer) {
    super(conf, null);
    this.setNumBspTask(numPeer);
  }

  public void ensureState(JobState state) throws IllegalStateException {
    if (state != this.state) {
      throw new IllegalStateException("Job in state " + this.state
          + " instead of " + state);
    }
  }

  // /////////////////////////////////////
  // Setter for Job Submission
  // /////////////////////////////////////
  public void setWorkingDirectory(Path pDir) {
    ensureState(JobState.DEFINE);
    Path dir = new Path(getWorkingDirectory(), pDir);
    conf.set(WORKING_DIR, dir.toString());
  }

  public void setWorkingDirectoryIfEmpty(Path dir) {
    if(null == getWorkingDirectory()) setWorkingDirectory(dir);
  }

  /**
   * Set the BSP algorithm class for the job.
   * 
   * @param cls the class implementing the BSP job
   * @throws IllegalStateException
   */
  @SuppressWarnings("rawtypes")
  @Deprecated
  public void setBspClass(Class<? extends BSP> cls) // TODO: remove
      throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setClass(WORK_CLASS_ATTR, cls, BSP.class);
  }

  /**
   * Set bsp based on the new BSP class.
   */
  public void setBSPClass(Class<? extends org.apache.hama.bsp.v2.BSP> clazz) {
    conf.setClass("bsp.work.class", clazz, org.apache.hama.bsp.v2.BSP.class);
  } 

  @SuppressWarnings("rawtypes")
  public void setSupersteps(Class<? extends Superstep>... classes) {
    ensureState(JobState.DEFINE);

    String clazzString = "";
    for (Class<? extends Superstep> s : classes)
      clazzString += s.getName() + ",";

    conf.set("hama.supersteps.class", clazzString);
    this.setBspClass(SuperstepBSP.class);
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public Class<? extends BSP> getBspClass() {
    return (Class<? extends BSP>) conf.getClass(WORK_CLASS_ATTR, BSP.class);
  }

  public void setCombinerClass(Class<? extends Combiner<? extends Writable>> cls) {
    ensureState(JobState.DEFINE);
    conf.setClass(Constants.COMBINER_CLASS, cls, Combiner.class);
  }

  @SuppressWarnings("unchecked")
  public Class<? extends Combiner<? extends Writable>> getCombinerClass() {
    return (Class<? extends Combiner<? extends Writable>>) conf.getClass(
        Constants.COMBINER_CLASS, Combiner.class);
  }

  public void setJar(String jar) {
    conf.set("bsp.jar", jar);
  }

  public void setJarByClass(Class<?> cls) {
    String jar = findContainingJar(cls);
    if (jar != null) {
      conf.set("bsp.jar", jar);
    }
  }

  private static String findContainingJar(Class<?> my_class) {
    ClassLoader loader = my_class.getClassLoader();
    String class_file = my_class.getName().replaceAll("\\.", "/") + ".class";
    try {
      for (Enumeration<URL> itr = loader.getResources(class_file); itr
          .hasMoreElements();) {

        URL url = itr.nextElement();
        if ("jar".equals(url.getProtocol())) {
          String toReturn = url.getPath();
          if (toReturn.startsWith("file:")) {
            toReturn = toReturn.substring("file:".length());
          }
          toReturn = URLDecoder.decode(toReturn, "UTF-8");
          return toReturn.replaceAll("!.*$", "");
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return null;
  }

  public void setJobName(String name) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.set("bsp.job.name", name);
  }

  public void setJobNameIfEmpty(final String name) {
    if(getJobName().isEmpty()) 
      setJobName(new Path(name).getName());
  }

  public void setUser(String user) {
    conf.set("user.name", user);
  }

  // /////////////////////////////////////
  // Methods for Job Control
  // /////////////////////////////////////
  public long progress() throws IOException {
    ensureState(JobState.RUNNING);
    return info.progress();
  }

  public boolean isComplete() throws IOException {
    ensureState(JobState.RUNNING);
    return info.isComplete();
  }

  public boolean isSuccessful() throws IOException {
    ensureState(JobState.RUNNING);
    return info.isSuccessful();
  }

  public void killJob() throws IOException {
    ensureState(JobState.RUNNING);
    info.killJob();
  }

  public void killTask(TaskAttemptID taskId) throws IOException {
    ensureState(JobState.RUNNING);
    info.killTask(taskId, false);
  }

  public void failTask(TaskAttemptID taskId) throws IOException {
    ensureState(JobState.RUNNING);
    info.killTask(taskId, true);
  }

  public void submit() throws IOException, InterruptedException {
    ensureState(JobState.DEFINE);
    info = jobClient.submitJob(this);
    state = JobState.RUNNING;
  }

  public boolean waitForCompletion(boolean verbose) throws IOException,
      InterruptedException, ClassNotFoundException {
    if (state == JobState.DEFINE) {
      submit();
    }

    if (verbose) {
      jobClient.monitorAndPrintJob(this, info);
    } else {
      info.waitForCompletion();
    }
    return isSuccessful();
  }

  // for the testcase
  BSPJobClient getJobClient() {
    return jobClient;
  }

  public void set(String name, String value) {
    conf.set(name, value);
  }

  public void setBoolean(String name, boolean value) {
    conf.setBoolean(name, value);
  }

  public boolean getBoolean(String name, boolean defaultValue) {
    return conf.getBoolean(name, defaultValue);
  }

  public void setNumBspTask(int tasks) {
    conf.setInt("bsp.peers.num", tasks);
  }

  public int getNumBspTask() {
    // default is 1, because with zero, we will hang in infinity
    return conf.getInt("bsp.peers.num", 1);
  }

  @SuppressWarnings({ "rawtypes" })
  public InputFormat getInputFormat() {
    return ReflectionUtils.newInstance(
        conf.getClass(Constants.INPUT_FORMAT_CLASS, TextInputFormat.class,
            InputFormat.class), conf);
  }

  @SuppressWarnings({ "rawtypes" })
  public void setInputFormat(Class<? extends InputFormat> cls) {
    conf.setClass(Constants.INPUT_FORMAT_CLASS, cls, InputFormat.class);
  }

  /**
   * Sets the compression codec that should be used to compress messages.
   */
  @SuppressWarnings({ "rawtypes" }) // TODO: remove
  @Deprecated
  public void setCompressionCodec(Class<? extends BSPMessageCompressor> clazz) {
    conf.setClass(BSPMessageCompressorFactory.COMPRESSION_CODEC_CLASS, clazz,
        BSPMessageCompressor.class);
  }

  /**
   * Set compression codec based on the new compressor class.
   */
  public void setCompressor(Class<? extends 
      org.apache.hama.message.compress.BSPMessageCompressor> clazz) {
    conf.setClass("hama.messenger.compression.class", clazz, 
                  org.apache.hama.message.compress.BSPMessageCompressor.class);
  }

  /**
   * Get the key class for the job input data.
   * 
   * @return the key class for the job input data.
   */
  public Class<?> getInputKeyClass() {
    return conf.getClass("bsp.input.key.class", LongWritable.class,
        Object.class);
  }

  /**
   * Set the key class for the job input data.
   * 
   * @param theClass the key class for the job input data.
   */
  public void setInputKeyClass(Class<?> theClass) {
    conf.setClass("bsp.input.key.class", theClass, Object.class);
  }

  /**
   * Get the value class for job input.
   * 
   * @return the value class for job input.
   */
  public Class<?> getInputValueClass() {
    return conf.getClass("bsp.input.value.class", Text.class, Object.class);
  }

  /**
   * Set the value class for job input.
   * 
   * @param theClass the value class for job input.
   */
  public void setInputValueClass(Class<?> theClass) {
    conf.setClass("bsp.input.value.class", theClass, Object.class);
  }

  /**
   * Get the key class for the job output data.
   * 
   * @return the key class for the job output data.
   */
  public Class<?> getOutputKeyClass() {
    return conf.getClass("bsp.output.key.class", LongWritable.class,
        Object.class);
  }

  /**
   * Set the key class for the job output data.
   * 
   * @param theClass the key class for the job output data.
   */
  public void setOutputKeyClass(Class<?> theClass) {
    conf.setClass("bsp.output.key.class", theClass, Object.class);
  }

  /**
   * Get the value class for job outputs.
   * 
   * @return the value class for job outputs.
   */
  public Class<?> getOutputValueClass() {
    return conf.getClass("bsp.output.value.class", Text.class, Object.class);
  }

  /**
   * Set the value class for job outputs.
   * 
   * @param theClass the value class for job outputs.
   */
  public void setOutputValueClass(Class<?> theClass) {
    conf.setClass("bsp.output.value.class", theClass, Object.class);
  }

  /**
   * Get the message class.
   * 
   * @return the message class.
   */
  public Class<?> getMessageClass() {
    return conf.getClass(Constants.MESSAGE_CLASS, Text.class, Object.class);
  }

  /**
   * Set the message class.
   * 
   * @param theClass the message class.
   */
  public void setMessageClass(Class<?> theClass) {
    conf.setClass(Constants.MESSAGE_CLASS, theClass, Object.class);
  }

  /**
   * Sets the output path for the job.
   * 
   * @param path where the output gets written.
   */
  public void setOutputPath(Path path) {
    conf.set("bsp.output.dir", path.toString());
  }

  /**
   * Sets the input path for the job.
   * @param path denotes the input directory to be set.
   */
  public void setInputPath(Path path) {
    conf.set("bsp.input.dir", path.toString());
  }

  /**
   * Check if the input directory is set.
   * @return boolean indicates whether input path exists or not. True when
   *                 path is set; otherwise false.
   */
  public boolean hasInputPath() {
    return (null != conf.get("bsp.input.dir"));
  }

  /**
   * Sets the output format for the job.
   */
  @SuppressWarnings("rawtypes")
  public void setOutputFormat(Class<? extends OutputFormat> theClass) {
    conf.setClass(Constants.OUTPUT_FORMAT_CLASS, theClass, OutputFormat.class);
  }

  /**
   * Sets the partitioner for the input of the job.
   */
  @SuppressWarnings("rawtypes")
  public void setPartitioner(Class<? extends Partitioner> theClass) {
    conf.setClass(Constants.RUNTIME_PARTITIONING_CLASS, theClass,
        Partitioner.class);
  }

  @SuppressWarnings("rawtypes")
  public OutputFormat getOutputFormat() {
    return ReflectionUtils.newInstance(conf.getClass(
        Constants.OUTPUT_FORMAT_CLASS, TextOutputFormat.class,
        OutputFormat.class), conf);
  }

  protected void setCheckPointInterval(int checkPointInterval) {
    conf.setInt(Constants.CHECKPOINT_INTERVAL, checkPointInterval);
  }

  protected void setCheckPointFlag(boolean enableCheckPoint) {
    conf.setBoolean(Constants.CHECKPOINT_ENABLED, enableCheckPoint);
  }

  /**
   * Set compression threshold in bytes.
   * 
   * @param ct
   */
  public void setCompressionThreshold(long ct) {
    conf.setLong("hama.messenger.compression.threshold", ct);
  }

  public void setMessageQueueBehaviour(String queueBehaviour) {
    if (queueBehaviour.equals(MessageQueue.PERSISTENT_QUEUE))
      conf.setBoolean(MessageQueue.PERSISTENT_QUEUE, true);
  }

  /**
   * Obtain replication value for this bsp job; value is set to 3.
   * Note that original job client set replication value to 10.
   * @return short value as the number of replication being set.
   */
  public short replication() {
    return (short) conf.getInt("bsp.submit.replication", 3); 
  }

  /**
   * Check if bsp.join.expr value exists or not. 
   * @return boolena denotes whether join expression exists; true if exists. 
   * Otherwise false.
   */
  public boolean hasJoinExpr() {
    return (null != conf.get("bsp.join.expr"));
  }

  /**
   * Check if input is partitioned.
   */
  public boolean hasPartitioned() {
    return conf.getBoolean("input.has.partitioned", false);
  }

  public void setGroupBy(final String user) {
    conf.set("group.name", user);
  }

}
