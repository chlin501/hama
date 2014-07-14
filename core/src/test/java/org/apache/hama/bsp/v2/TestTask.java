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

import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.io.DataInputStream;

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.HamaConfiguration;


/**
 * Test (De)Serialize functions.
 */
public class TestTask extends TestCase {

  final Log LOG = LogFactory.getLog(TestTask.class);

  Task createTaskWithDefault(final int id) throws Exception {
    final TaskAttemptID attemptId = IDCreator.newBSPJobID().
                                              withId("test").
                                              withId(id).
                                              getTaskIDBuilder().
                                              withId(id).
                                              getTaskAttemptIDBuilder().
                                              withId(id).
                                              build();
    final HamaConfiguration conf = new HamaConfiguration();
    final Task task = new Task.Builder().setId(attemptId). 
                                         setConfiguration(conf).
                                         build();
    return task;
  }

  Task createTask(final int id) throws Exception {
    final TaskAttemptID attemptId = IDCreator.newBSPJobID().
                                              withId("test").
                                              withId(id).
                                              getTaskIDBuilder().
                                              withId(id).
                                              getTaskAttemptIDBuilder().
                                              withId(id).
                                              build();
    final HamaConfiguration conf = new HamaConfiguration();
    final long startTime = System.currentTimeMillis();
    final long finishTime = startTime + 1000*10;
    LOG.info("Note that State is configured to RUNNING!!!");
    final Task.State state = Task.State.RUNNING;
    final Task.Phase phase = Task.Phase.SETUP;
    final Task task = new Task.Builder().setId(attemptId). 
                                         setConfiguration(conf).
                                         setStartTime(startTime).
                                         setFinishTime(finishTime).
                                         setState(state).
                                         setPhase(phase).
                                         setCompleted(true).
                                         build();
    return task;
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

  Task deserialize(byte[] bytes) throws Exception {
    final ByteArrayInputStream bin = new ByteArrayInputStream(bytes);
    final DataInputStream in = new DataInputStream(bin);
    final Task task = new Task();
    try {
      task.readFields(in);
    } finally {
      in.close();
    } 
    return task;
  }

  public void testSerialization() throws Exception {
    final int id = 1;
    final Task task = createTask(id);
    final byte[] bytes = serialize(task);
    final Task forVerification = deserialize(bytes);
    LOG.info("Restored TaskID is "+forVerification.getId());
    assertEquals("TaskID should be the same.", 
                 task.getId(), forVerification.getId());

    LOG.info("Restored startTime is "+forVerification.getStartTime());
    assertEquals("Start time should be "+task.getStartTime(), 
                 task.getStartTime(), forVerification.getStartTime());

    LOG.info("Restored finishTime is "+forVerification.getFinishTime());
    assertEquals("Finish time should be "+task.getFinishTime(), 
                 task.getFinishTime(), forVerification.getFinishTime());

    assertEquals("State should be "+task.getState().toString(), 
                 task.getState(), forVerification.getState()); 
    assertEquals("Phase should be "+task.getPhase().toString(), 
                 task.getPhase(), forVerification.getPhase()); 

    LOG.info("Restored isCompleted is "+forVerification.isCompleted());
    assertEquals("Completed should be "+task.isCompleted(), 
                 task.isCompleted(), forVerification.isCompleted());
  }

  /**
   * Test moving phase/ state to the next one is correctly configured.
   */
  public void testStage() throws Exception {
    final Task task = createTaskWithDefault(9);
    assertPhase(task, Task.Phase.SETUP);
    assertState(task, Task.State.WAITING);
    nextStage(task);
    assertPhase(task, Task.Phase.COMPUTE);
    assertState(task, Task.State.RUNNING);
    nextStage(task);
    assertPhase(task, Task.Phase.BARRIER_SYNC);
    assertState(task, Task.State.SUCCEEDED);
    nextStage(task);
    assertPhase(task, Task.Phase.CLEANUP);
    assertState(task, Task.State.FAILED);
    nextStage(task);
    assertPhase(task, Task.Phase.SETUP); // must start from the initial phase
    assertState(task, Task.State.STOPPED);
    nextStage(task);
    assertPhase(task, Task.Phase.COMPUTE);
    assertState(task, Task.State.CANCELLED);
    nextStage(task);
    assertPhase(task, Task.Phase.BARRIER_SYNC);
    assertState(task, Task.State.WAITING); // must start from the initial state
  }

  void nextStage(final Task task) throws Exception {
    task.nextPhase();
    task.nextState();
  }

  void assertPhase(final Task task, 
                   final Task.Phase expectedPhase) throws Exception {
    final Task.Phase actualPhase = task.getPhase();
    LOG.info("Current phase "+actualPhase);
    assertNotNull("Phase shoudn't be null!", actualPhase);
    assertEquals("Initial phase should be "+expectedPhase, 
                 expectedPhase, actualPhase);
  }

  void assertState(final Task task, 
                   final Task.State expectedState) throws Exception {
    final Task.State actualState = task.getState();
    LOG.info("Current state "+actualState);
    assertNotNull("State shoudn't be null!", actualState);
    assertEquals("Initial State should be "+expectedState, 
                 expectedState, actualState);
  }
}
