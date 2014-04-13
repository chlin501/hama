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

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Writable;

import org.apache.hama.bsp.BSPJobClient;
import org.apache.hama.bsp.BSPJobClient.RawSplit;
import org.apache.hama.bsp.BSPJobID;
import org.apache.hama.HamaConfiguration;

public final class TaskTable implements Writable {

  final Log LOG = LogFactory.getLog(TaskTable.class);

  /* Indicate to which job id this task table belongs. */
  private BSPJobID jobId; 

  /* Hard bound check for task restart. */
  private int maxTaskAttempts;

  /* The number of tasks allowed for a job. */
  private int numBSPTasks;

  /* An array of tasks, reprenting the task table. */
  private ArrayWritable[] tasks;

  TaskTable() {} // for Writable

  public TaskTable(final BSPJobID jobId, final HamaConfiguration conf) {
    this(jobId, conf.getInt("bsp.peers.num", 1), 
         conf.getInt("bsp.job.task.retry_n_times", 1), null); 
  }

  /**
   * Initialize a 2d task array with numBSPTasks rows, and a task in column.
   * When a task fails, that task may attempt to re-execute several times, with 
   * max retry up to maxTaskAttempts.  
   * @pram BSPJobID indicates to which job this table belongs.
   * @param numBSPTasks specifies the row or the number of tasks this table can
   *                    have.
   * @param maxTaskAttempts denotes the max retry that a task can have.
   * @param splits denotes the data splits to be consumed by each task.
   */ 
  public TaskTable(final BSPJobID jobId, 
                   final int numBSPTasks, 
                   final int maxTaskAttempts, 
                   final BSPJobClient.RawSplit[] splits) {
    this.jobId = jobId;
    if(null == this.jobId)
      throw new IllegalArgumentException("TaskTable's BSPJobID is missing!");

    this.numBSPTasks = numBSPTasks;
    if(0 >= this.numBSPTasks) 
      throw new IllegalArgumentException("numBSPTasks is not valid.");

    // TODO: restrict the numBSPTasks value so that the task array allocated
    //       won't explode!

    this.maxTaskAttempts = maxTaskAttempts;
    if(0 >= this.maxTaskAttempts) 
      throw new IllegalArgumentException("maxTaskAttempts is not valid.");

    final BSPJobClient.RawSplit[] rawSplits = splits;
    // we can't assert numBSPTasks value against splits length because
    // there may not have splits provided (meaning null == splits)!
    // and each task is assigned with null split. 
    if(null != rawSplits && 0 < rawSplits.length) {
      this.numBSPTasks = rawSplits.length;
      LOG.info("Adjusting numBSPTasks to "+numBSPTasks);
    } 

    // init tasks
    this.tasks = new ArrayWritable[numBSPTasks];
    for(int row = 0; row < numBSPTasks; row++) {
      this.tasks[row] = new ArrayWritable(Task.class);
      final BSPJobClient.RawSplit split = (null != rawSplits && 
                                           0 < rawSplits.length)?
                                           rawSplits[row]:null;
      set(row, new Task[] {
        new Task.Builder().setId(IDCreator.newTaskID()
                                          .withId(getJobId())
                                          .withId((row+1)) // TaskID's id
                                          .getTaskAttemptIDBuilder()
                                          .withId(1) // TaskAttemptID's id
                                          .build())
                          .setPhase(Task.Phase.SETUP)
                          .setState(Task.State.WAITING) 
                          .setPartition((row+1))
                          .setSplit(split)
                          .build()
      }); 
    }
    LOG.info("TaskTable for "+numBSPTasks+" tasks is initialized.");
  }

  /* Row index is started from 0. */
  boolean isValidRow(final int row) {
    if(row >= numBSPTasks) return false; else return true;
  }

  /* Column index is started from 0. */
  boolean isValidColumn(final int column) {
    if(column >= maxTaskAttempts) return false; else return true;
  }

  boolean isValidPosition(final int row, final int column) {
    return isValidRow(row) && isValidColumn(column);
  }

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

  public int getNumBSPTasks() {
    return this.numBSPTasks;
  }
  
  public int columnLength() {
    return getMaxTaskAttempts();
  }

  public int getMaxTaskAttempts() {
    return this.maxTaskAttempts;
  }

  /** 
   * Obtain a task array that may contain restart attempt for a particular row.
   * @param row is the i-th position in the task table.
   * @return Task[] with length at least set to 1; otherwise null if invalid 
   *                row parameter is passed in.
   */ 
  public Task[] get(final int row) {
    return (Task[])this.tasks[row].get();
  }

  /**
   * Set the task array to a specific row.
   * @param row denotes the i-th row of the table.
   * @param tasks are the entire tasks to be retried, including the init task.
   */
  public void set(final int row, final Task[] tasks) {
    this.tasks[row].set(tasks);
  }

  public void set(final int row, final int column, final Task task) {
    final Task[] tasks = get(row);
    tasks[column] = task;
  }

  public int sizeAt(final int row) {
    final Task[] taskAttemptArray = get(row); 
    if(null == taskAttemptArray) return -1;
    return taskAttemptArray.length;
  }

  /**
   * Obtain a specific Task from TaskTable.
   * @param row is the numBSPTasks
   * @param column is the maxTaskAttempts.
   */
  public Task get(final int row, final int column) {
    if(!isValidPosition(row, column)) return null;
    final Task[] taskAttemptArray= get(row);
    if(null == taskAttemptArray) return null;
    return taskAttemptArray[column];
  }

  /** 
   * Resize the column length for a particular row in this task table.
   * Previous task array information such as should be retained/ copied.
  public void resizeTo(final int rowAt, final int size) {
    if(!isValidColumn(size)) return;
    final Task[] taskAttemptArray = get(row); 
    if(null == taskAttemptArray) return;
    final Task[] newTaskAttemptArray = new Task[size];
    for(int idx = 0; idx < size; idx++) {
      if(taskAttemptArray.length > idx) {
        newTaskAttemptArray[idx] = taskAttemptArray[idx];
      }
    }
  }
   */ 

  /**
   * The position of row and column is started from 0.
   * if row exceeds numBSPTasks or column exceeds maxTaskAttempts, task will 
   * not be assigned.
  public void assign(final int row, final int column, final Task task) {
    if(!isValidPosition(row, column)) return;
    final Task[] taskAttemptArray = get(row);
    if(null == taskAttemptArray) return;
    taskAttemptArray[column] = task;
  }

  public void assign(final int row, final Task[] tasks) {
    if(!isValidRow(row)) return;
    this.set(row, tasks);
  }
   */

  /**
   * Add a (restarted) task to the end of a designated row.
   */
  public void add(final int row, final Task task) {
    if(!isValidRow(row)) return;
    final Task[] taskAttemptArray = get(row);
    if(null == taskAttemptArray) return;
    final Task[] tmpTasks = new Task[taskAttemptArray.length+1];
    for(int idx = 0; idx < tmpTasks.length; idx++) {
      if(idx < (taskAttemptArray.length-1) ) {
        tmpTasks[idx] = taskAttemptArray[idx];
      } else {
        tmpTasks[idx] = task;
      }
    }
    this.set(row, tmpTasks); 
  }

  /**
   * Remove the latest task from a designed row.
   * @param row at which the latest task will be removed.
   */
  public void remove(final int row) {
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
  public Task nextUnassignedTask() {
    for(int idx = 0; idx < rowLength(); idx++) {
      final Task task = get(idx, 0);
      if(null == task) 
        throw new RuntimeException("The task at row: "+idx+" not found!");
      if(!task.isAssigned()) return task;
    }
    return null;
  }

  public boolean areAllTasksAssigned() {
    int count = 0;
    for(int idx = 0; idx < rowLength(); idx++) {
      final Task task = get(idx, 0);
      if(null == task) 
        throw new RuntimeException("The task at row: "+idx+" not found!");
      if(task.isAssigned()) count++;
    }
    if(rowLength() == count) return true; else return false;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    this.jobId.write(out);
    out.writeInt(tasks.length); // numBSPTasks                  
    for (int row = 0; row < tasks.length; row++) {
      final int columnLength = sizeAt(row);
      if(-1 == columnLength) 
        throw new IOException("Fail retrieving column length for the "+row+
                              "-th row.");
      out.writeInt(columnLength); // maxTaskAttempts
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
    this.jobId.readFields(in);
    final int row = in.readInt();
    if(this.numBSPTasks != row) 
      throw new IllegalStateException("NumBSPTasks "+numBSPTasks+" and row "+
                                      row+" not match.");
    this.tasks = new ArrayWritable[row];
    for (int rowIdx = 0; rowIdx < tasks.length; rowIdx++) {
      final int columnLength = in.readInt();
      this.tasks[rowIdx] = new ArrayWritable(Task.class);
      this.tasks[rowIdx].set(new Task[columnLength]);
    }

    for (int rowIdx = 0; rowIdx < tasks.length; rowIdx++) {
      for (int column = 0; column < tasks[rowIdx].get().length; column++) {
        final Task task = new Task();
        task.readFields(in);
        this.tasks[rowIdx].get()[column] = task; 
      }
    }
  }

  @Override
  public String toString() {
    if(null != this.tasks) {
      final StringBuilder sb = new StringBuilder();
      for(final ArrayWritable taskArray: this.tasks) {
        final Writable[] retryTasks = taskArray.get();
        if(null != retryTasks && 0 < retryTasks.length) {
          sb.append(((Task)retryTasks[0]).getId()+" ");
        }
      }
      return sb.toString();
    } else return "<empty tasks>";
  }
}
