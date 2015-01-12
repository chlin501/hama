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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hama.SystemInfo;
import org.apache.hama.bsp.BSPJobID;
import org.apache.hama.bsp.TaskID;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.io.PartitionedSplit;

public final class TaskTable implements Writable {

  final Log LOG = LogFactory.getLog(TaskTable.class);

  /* Indicate to which job id this task table belongs. */
  private BSPJobID jobId; 

  /* This variable, derived from v2.Job, contains specific setting for a job. */
  private HamaConfiguration configuration = new HamaConfiguration();

  /* An array of tasks, reprenting the task table. */
  private ArrayWritable[] tasks;

  TaskTable() {} // for Writable

  public TaskTable(final BSPJobID jobId, final HamaConfiguration conf) {
    this(jobId, conf, null); 
  }

  /**
   * Initialize a 2d task array with numBSPTasks rows, and a task in column.
   *
   * Retried Task will not be created during initialization in reducing objects
   * created. 
   *
   * When a task fails, that task may attempt to re-execute several times, with 
   * max retry up to <b>maxTaskAttempts</b>.  
   *
   * @pram BSPJobID indicates to which job this table belongs.
   * @param splits denotes the data splits to be consumed by each task.
   */ 
  // TODO: perhaps use other data structure to record tasks for efficiency. 
  public TaskTable(final BSPJobID jobId, 
                   final HamaConfiguration conf,
                   final PartitionedSplit[] splits) {
    this.jobId = jobId;
    if(null == this.jobId)
      throw new IllegalArgumentException("TaskTable's BSPJobID is missing!");
   
    this.configuration = conf;
    if(null == this.configuration)
      throw new IllegalArgumentException("HamaConfiguration for job id "+
                                         this.jobId.toString()+" is missing!");

    final PartitionedSplit[] rawSplits = splits;
    if(hasSplit(rawSplits)) {
      this.configuration.setInt("bsp.peers.num", rawSplits.length);
      LOG.info("Adjusting numBSPTasks to "+rawSplits.length);
    }  

    // init tasks
    final int numBSPTasks = getNumBSPTasks();
    this.tasks = new ArrayWritable[numBSPTasks];
    for(int row = 0; row < numBSPTasks; row++) {
      this.tasks[row] = new ArrayWritable(Task.class);
      final PartitionedSplit split = hasSplit(rawSplits)? rawSplits[row]:null;
      set(row, new Task[] {
        new Task.Builder().setId(IDCreator.newTaskID()
                                          .withId(getJobId())
                                          .withId((row+1)) // TaskID's id
                                          .getTaskAttemptIDBuilder()
                                          .withId(1) // TaskAttemptID's id
                                          .build()).
                           setConfiguration(conf).
                           setSplit(split).
                           build()
      }); 
    }
    LOG.info("TaskTable for "+jobId.toString()+" has "+numBSPTasks+
             " tasks initialized.");
  }

  /**
   * Check if there are splits.
   * @param rawSplits are data to be consumed as input.
   * @return boolean will either returns true if having splits; false otherwise.
   */
  boolean hasSplit(final PartitionedSplit[] rawSplits) {
    return (null != rawSplits && 0 < rawSplits.length);
  }

  /* Row index is started from 0. */
  boolean isValidRow(final int row) {
    if(row >= getNumBSPTasks()) return false; else return true;
  }

  /* Column index is started from 0. */
  boolean isValidColumn(final int column) {
    if(column >= getMaxTaskAttempts()) return false; else return true; 
  }

  boolean isValidPosition(final int row, final int column) {
    return isValidRow(row) && isValidColumn(column);
  }

  /**
   * BSPJobID to which this task table belongs.
   * @return BSPJobID for this task table. 
   */
  public BSPJobID getJobId() {
    return this.jobId;
  }

  /**
   * The row (numBSPTasks) of this task table.
   * @return int 
   */
  public int rowLength() {
    return getNumBSPTasks();
  }

  /**
   * The number of BSP tasks in this task table. 
   * @return int denotes the number of the BSP tasks.
   */
  public int getNumBSPTasks() {
    return this.configuration.getInt("bsp.peers.num", 1);
  }
  
  /**
   * This value denotes the max retry a task can have. Not all task will use
   * up all retry.
   * @param row denotes the N-th row, started from 0, in the task table. 
   * @return int denotes the max value of a task retry.
   */
  int columnLength(final int row) {
    return sizeAt(row);
  }

  public int getMaxTaskAttempts() { 
    return this.configuration.getInt("bsp.tasks.max.attempts", 3);
  }

  /** 
   * Obtain a task array that may contain restart attempt for a particular row.
   * @param row is the i-th position in the task table.
   * @return Task[] with length at least set to 1; otherwise null if invalid 
   *                row parameter is passed in.
   */ 
  Task[] get(final int row) {
    if(0 > row || rowLength() <= row) 
      throw new IllegalArgumentException("Invalid row value: "+row +". Total "+
                                         "row length is "+rowLength());
    return (Task[])this.tasks[row].get();
  }

  /**
   * Set the task array to a specific row.
   * @param row denotes the i-th row, started from 0, of the table.
   * @param tasks are the entire tasks to be retried, including the init task.
   */
  void set(final int row, final Task[] tasks) {
    if(0 > row || rowLength() <= row) 
      throw new IllegalArgumentException("Invalid row value: "+row +". Total "+
                                         "row length is "+rowLength());
    this.tasks[row].set(tasks);
  }

  void set(final int row, final int column, final Task task) {
    if(0 > row || rowLength() <= row) 
      throw new IllegalArgumentException("Invalid row value: "+row +". Total "+
                                         "row length is "+rowLength());
    final Task[] tasks = get(row);
    tasks[column] = task;
  }

  /**
   * Tell the size of tasks at N-th row.
   * @param row dentoes the N-th row, started from 0, in the task table.
   */
  int sizeAt(final int row) {
    final Task[] taskAttemptArray = get(row); 
    if(null == taskAttemptArray) return -1;
    return taskAttemptArray.length;
  }

  /**
   * Find tasks matching to host, port supplied.
   * Matched tasks denote tasks running on the node with host and port values
   * given. 
   * @param Task[] is an array of tasks matching to host and port values given.
   */
  List<Task> findTasksBy(final String host, final int port) {
    final List<Task> matched = new ArrayList<Task>();
    for (int row = 0; row < rowLength(); row++) {
      final Task task = latestTaskAt(row);
      if(null == task) 
        throw new NullPointerException("No task found at row "+row);
      if(task.getAssignedHost().equals(host) && 
         (port == task.getAssignedPort())) matched.add(task);
    }
    return Collections.unmodifiableList(matched);
  }

  /**
   * Retrieve the latest task at the N-th row.
   * @param row dentoes the N-th row, started from 0, in the task table.
   */
  Task latestTaskAt(final int row) {
    final Task[] taskAttemptArray = get(row); 
    if(null == taskAttemptArray) return null;
    return taskAttemptArray[taskAttemptArray.length-1];
  }

  /**
   * Return the latest tasks that is currently running.
   */
  List<Task> latestTasks() {
    final List<Task> latestTasks = new ArrayList<Task>();
    for (int row = 0; row < rowLength(); row++) {
      final Task latest = latestTaskAt(row);
      latestTasks.add(latest);  
    } 
    return Collections.unmodifiableList(latestTasks);
  }

  Set<SystemInfo> grooms() {
    final Set<SystemInfo> infos = new HashSet<SystemInfo>();
    for(final Task latest: latestTasks()) {
      infos.add(latest.runsAt());
    }
    return Collections.unmodifiableSet(infos);
  }

  /**
   * Group tasks by target GroomServer's name.
   * @return Map contains GroomServer name as key, and GroomServer count as
   *             value.
   */
  // TODO: performance?
  Map<String, Integer> group() {
    final Map<String, Integer> cache = new HashMap<String, Integer>();
    for(int row=0;row<rowLength(); row++) {
      final Task task = latestTaskAt(row);
      if(null == task) 
        throw new RuntimeException("The lastest task at "+row+" is not found!");
      final String hostPort = task.getAssignedHostPort();
      final Integer count = cache.get(hostPort);
      if(null == count) {
        cache.put(hostPort, new Integer(1)); 
      } else {
        cache.put(hostPort, new Integer(count.intValue()+1));
      }
    }
    return cache;
  }

  /**
   * Obtain a specific Task from TaskTable.
   * @param row is the numBSPTasks
   * @param column is the maxTaskAttempts.
   */
  Task get(final int row, final int column) {
    if(!isValidPosition(row, column)) return null;
    final Task[] taskAttemptArray= get(row);
    if(null == taskAttemptArray) return null;
    return taskAttemptArray[column];
  }

  /**
   * Add a new task to the end of a designated row.
   * @param row of the task table. 
   * @param newTask to be added.
   */
  void add(final int row, final Task newTask) {
    if(!isValidRow(row)) 
      throw new IllegalArgumentException("Invalid row number "+row);
    final Task[] taskAttemptArray = get(row);
    if(null == taskAttemptArray) 
      throw new NullPointerException("Attempt tasks at row "+row+" is empty!"); 
    final int maxAttemptAllowed = getMaxTaskAttempts(); 
    if(maxAttemptAllowed < taskAttemptArray.length) 
      throw new ExceedMaxTaskAllowedException(newTask.getId().getJobID().
      toString(), maxAttemptAllowed);
    final Task[] tmpTasks = new Task[taskAttemptArray.length+1];
    for(int idx = 0; idx < tmpTasks.length; idx++) {
      if(idx < (taskAttemptArray.length-1) ) {
        tmpTasks[idx] = taskAttemptArray[idx];
      } else {
        tmpTasks[idx] = newTask;
      }
    }
    this.set(row, tmpTasks); 
  }

  /**
   * Append the newest task to the end of the corresponded column, i.e. task id
   * are equal, if not exceeding the max task attempt allowed.
   */
  void add(final Task newTask) {
    final TaskID id = newTask.getId().getTaskID();
    for (int row = 0; row < rowLength(); row++) {
      final Task old = latestTaskAt(row);
      if(null == old) 
        throw new NullPointerException("The latest task is empty at row "+row);
      if(old.getId().getTaskID().equals(id)) {
        final Task[] oldAttempts = get(row);    
        if(null == oldAttempts) 
          throw new NullPointerException("Attempt tasks at row " + row + 
                                         "is empty!"); 
        final int maxAttemptAllowed = getMaxTaskAttempts(); 
        if(maxAttemptAllowed < oldAttempts.length) 
          throw new ExceedMaxTaskAllowedException(newTask.getId().getJobID().
          toString(), maxAttemptAllowed);
        final Task[] tmpAttempts = new Task[oldAttempts.length+1];
        for(int idx = 0; idx < tmpAttempts.length; idx++) {
          if(idx < (oldAttempts.length-1) ) {
            tmpAttempts[idx] = oldAttempts[idx];
          } else {
            tmpAttempts[idx] = newTask;
          }
        }
        this.set(row, tmpAttempts);  
      }
    }
  }

  boolean markAsCancelled(final TaskAttemptID taskAttemptId) {
    boolean flag = false;
    for (int row = 0; row < rowLength(); row++) {
      final Task latest = latestTaskAt(row);
      if(null == latest) 
        throw new NullPointerException("Task at row "+row+" is null!");
      if(latest.getId().equals(taskAttemptId)) {
        latest.cancelledState();
        flag = true;
        break;
      }
    }
    return flag;
  }

  boolean allTasksStopped() {
    int count = 0;
    for (int row = 0; row < rowLength(); row++) {
      final Task latest = latestTaskAt(row);
      if(null == latest) 
        throw new NullPointerException("Task at row "+row+" is null!");
      if(latest.isCancelled() || latest.isFailed()) count += 1;
    }
    return ((count + 1) == rowLength());
  }

  /**
   * Remove the latest task from a designed row.
   * @param row at which the latest task will be removed.
   */
  void remove(final int row) {
    if(!isValidRow(row)) return;
    final Task[] taskAttemptArray = get(row);
    final Task[] tmpTasks = new Task[taskAttemptArray.length-1];
    for(int idx = 0; idx < tmpTasks.length; idx++) {
      tmpTasks[idx] = taskAttemptArray[idx];
    }
    this.set(row, tmpTasks); 
  }

  /**
   * Find the next task unassigned to Groom.
   * @return Task that is not yet assigned to a GroomServer; null if all tasks
   *              are already assigned to GroomServers.
   */
  Task nextUnassignedTask() {
    for(int row = 0; row < rowLength(); row++) {
      final int lastTaskPos = (columnLength(row) - 1);
      final Task task = get(row, lastTaskPos); 
      if(null == task) 
        throw new RuntimeException("The last task at row "+row+" not found!");
      if(!task.isAssigned()) return task;
    }
    return null;
  }

/*
  boolean areAllTasksAssigned() {
    int count = 0;
    for(int idx = 0; idx < rowLength(); idx++) {
      final int lastTaskPos = (columnLength(row) - 1);
      final Task task = get(idx, lastTaskPos);
      if(null == task) 
        throw new RuntimeException("The task at row: "+idx+" not found!");
      if(task.isAssigned()) count++;
    }
    if(rowLength() == count) return true; else return false;
  }
*/

  @Override
  public void write(DataOutput out) throws IOException {
    this.jobId.write(out);
    this.configuration.write(out); // conf
    out.writeInt(tasks.length); // numBSPTasks 
    for (int row = 0; row < tasks.length; row++) {
      final int columnLength = sizeAt(row);
      if(0 >= columnLength)  // at least 1 task during init.
        throw new IOException("Invalid column length for the "+row+"-th row.");
      out.writeInt(columnLength); // actual task attempts
    }
    for (int row = 0; row < tasks.length; row++) {
      for (int column = 0; column < sizeAt(row); column++) {
        final Task task = get(row, column); 
        if(null != task)  task.write(out);
      }
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.jobId = new BSPJobID();
    this.jobId.readFields(in); // restore job id
    this.configuration = new HamaConfiguration();
    this.configuration.readFields(in); // restore conf
    final int row = in.readInt(); // restore numBSPTasks
    this.tasks = new ArrayWritable[row];
    for (int rowIdx = 0; rowIdx < tasks.length; rowIdx++) {
      final int columnLength = in.readInt();
      this.tasks[rowIdx] = new ArrayWritable(Task.class);
      this.tasks[rowIdx].set(new Task[columnLength]);
    }
    for (int rowIdx = 0; rowIdx < tasks.length; rowIdx++) {
      for (int colIdx = 0; colIdx < tasks[rowIdx].get().length; colIdx++) {
        final Task task = new Task();
        task.readFields(in);
        this.tasks[rowIdx].get()[colIdx] = task; 
      }
    }
  }

  @Override
  public String toString() {
    if(null != this.tasks) {
      final StringBuilder sb = new StringBuilder("TaskTable(");
      int idx = 0;
      for(final ArrayWritable taskArray: this.tasks) {
        final Writable[] retryTasks = taskArray.get();
        if(null != retryTasks && 0 < retryTasks.length) {
          sb.append(((Task)retryTasks[retryTasks.length-1]).getId());
          if(idx < rowLength()) sb.append(",");
        } 
        idx++;
      }
      sb.append(")");
      return sb.toString();
    } else return "TaskTable(<empty tasks>)";
  }
}
