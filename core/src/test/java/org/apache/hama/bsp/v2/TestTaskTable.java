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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.BSPJobID;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.HamaConfiguration;


public class TestTaskTable extends TestCase {

  final Log LOG = LogFactory.getLog(TestTaskTable.class);

  final HamaConfiguration conf = new HamaConfiguration();
  final int numBSPTasks = 3;
  final int maxTaskAttempts = 2;

  public void setUp() throws Exception {
    conf.setInt("bsp.peers.num", numBSPTasks);
    conf.setInt("bsp.tasks.max.attempts", maxTaskAttempts); 
  } 

  /**
   * Create a task table with tasks in the first slots initialized.
   * TaskTable's row length is numBSPTasks and max column length is 2.
   */
  TaskTable createTaskTable(final BSPJobID jobId, 
                            final HamaConfiguration conf) throws Exception {
    final TaskTable table = new TaskTable(jobId, conf, null);
    // assert init
    for(int row = 0; row < numBSPTasks; row++) {
      final Task[] taskArray = table.get(row);
      assertNotNull("Ensure task array is initialized.", taskArray);
      assertEquals("Initial task array size should be 1.", taskArray.length, 1);
      assertNotNull("A default task should be allocated.", taskArray[0]);
      final Task task = taskArray[0];
      assertEquals("Initial Task.Phase should be SETUP.", 
                   Task.Phase.SETUP, task.getPhase());
      final int taskID_id = task.getId().getTaskID().getId(); 
      LOG.info("TaskID's id is "+taskID_id);
      assertEquals("TaskID's id should be "+(row+1), (row+1), taskID_id);
      final int attemptID_id = task.getId().getId();
      LOG.info("TaskAttemptID's id is "+attemptID_id);
      assertEquals("TaskAttemptID's id should be 1.", 1, attemptID_id);
    } 
    return table;
  }

  byte[] serialize(Writable writable) throws Exception {
    final ByteArrayOutputStream bout = new ByteArrayOutputStream();
    final DataOutputStream out = new DataOutputStream(bout);
    try {
      writable.write(out);
    } finally {
      out.close();
    }
    return bout.toByteArray();
  }

  TaskTable deserialize(byte[] bytes) throws Exception {
    final ByteArrayInputStream bin = new ByteArrayInputStream(bytes);
    final DataInputStream in = new DataInputStream(bin);
    final TaskTable table = new TaskTable();
    try { 
      table.readFields(in);
    } finally {
      in.close();
    }
    return table;
  }

  BSPJobID createBSPJobId() throws Exception {
    return IDCreator.newBSPJobID().withId("test").withId(1).build();
  }

  public void testSerialization() throws Exception {
    final TaskTable table = createTaskTable(createBSPJobId(), conf);
    final byte[] bytes = serialize(table);
    final TaskTable forVerification = deserialize(bytes);
    LOG.info("Table table row length is "+forVerification.rowLength());
    assertEquals("TaskTable row length should be "+numBSPTasks, 
                 numBSPTasks, forVerification.rowLength());
    LOG.info("Table table's max task attempts is "+
             forVerification.getMaxTaskAttempts());
    assertEquals("TaskTable's max task attempts should be "+maxTaskAttempts,  
                 maxTaskAttempts, forVerification.getMaxTaskAttempts());
    final Task[] tasks = forVerification.get(1);
    assertEquals("The 2th row's size should be 1.", 1, tasks.length); 
    final Task assignedTask  = tasks[0];
    assertNotNull("Task assigned should not be null.", assignedTask);
    final Task task_at_1_1 = table.get(1)[0];
    assertEquals("Task's whole id should be "+task_at_1_1.getId().toString(), 
                 assignedTask.getId().toString(), 
                 task_at_1_1.getId().toString());
    assertEquals("Task attempt id should be "+task_at_1_1.getId().getId(), 
                 assignedTask.getId().getId(), task_at_1_1.getId().getId());
  }

  public void testTaskManagement() throws Exception {
    final BSPJobID jobId = createBSPJobId();
    final TaskTable table = createTaskTable(jobId, conf);
   
    // add a (restart) task.
    // task table should look like 
    // row [(TaskID's id, TaskAttemptID's id)]
    //
    //   0 [(1,1)]
    //   1 [(2,1), (2,2)]  
    //   2 [(3,1)]
    final TaskAttemptID attemptId = IDCreator.newTaskID().withId(jobId).
                                              withId(2).
                                              getTaskAttemptIDBuilder().
                                              withId(2).
                                              build();
    final Task task = new Task.Builder().setId(attemptId).
                                         setConfiguration(conf).
                                         setPhase(Task.Phase.COMPUTE).
                                         setState(Task.State.RUNNING).
                                         build();
    table.add(1, task);

    final Task[] tasks0 = table.get(0);
    assertEquals("The size of the 1st row should be 1.", 1, tasks0.length);
    final Task[] tasks1 = table.get(1);
    assertEquals("The size of the 2nd row should be 2.", 2, tasks1.length);
    final Task runningTask = tasks1[1];
    assertNotNull("Task can't be null.", runningTask);

    final TaskAttemptID tasks1_1 = runningTask.getId(); // attempt id
    LOG.info("The 2nd task in the 2nd row: "+tasks1_1.toString()); 
    assertEquals("TaskAttemptID should be "+attemptId.toString(), 
                 attemptId.toString(), tasks1_1.toString());   

    assertEquals("TaskID should be "+attemptId.getTaskID().toString(), 
                 attemptId.getTaskID().toString(), 
                 tasks1_1.getTaskID().toString());   

    assertEquals("BSPJobID should be "+
                 attemptId.getTaskID().getJobID().toString(), 
                 attemptId.getTaskID().getJobID().toString(), 
                 tasks1_1.getTaskID().getJobID().toString());   

    assertEquals("TaskID's id should be 2.", 2, tasks1_1.getId());

    assertEquals("Task phase should be "+Task.Phase.COMPUTE.toString(), 
                 Task.Phase.COMPUTE, runningTask.getPhase());

    assertEquals("Task state should be "+Task.State.RUNNING.toString(), 
                 Task.State.RUNNING, runningTask.getState());

    final Task[] tasks2 = table.get(2);
    assertEquals("The size of the 3rd row should be 1.", 1, tasks2.length);

    // remove a task.
    // task table after removed should look like 
    // row [(TaskID's id, TaskAttemptID's id)]
    //
    //   0 [(1,1)]
    //   1 [(2,1)]  
    //   2 [(3,1)]
    table.remove(1); 
    for(int row = 0; row < table.rowLength(); row++) {
      final Task[] tasks = table.get(0);
      assertNotNull("Task array at row "+row+" can't be null.", tasks);
      LOG.info("Task length for row "+row+" is "+tasks.length);
      assertEquals("Task array length for row "+row+" should be 1.",
                   1, tasks.length);
    }
  }
}
