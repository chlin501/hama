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

import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.BSPJobID;
import org.apache.hama.bsp.Counters;
import org.apache.hama.bsp.JobStatus;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.bsp.TaskID;
import org.apache.hama.bsp.TaskStatus;
import org.apache.hama.bsp.Counters;

/**
 * Test (De)Serialize functions.
 */
public class TestJob extends TestCase {

  final Log LOG = LogFactory.getLog(TestJob.class);
  final int numBSPTasks = 1024;
  final int maxTaskAttempts = 4;
  final HamaConfiguration conf = new HamaConfiguration();

  BSPJobID createJobId(final String prefix, final int id) throws Exception {
    return IDCreator.newBSPJobID().withId(prefix).withId(id).build();
  }

  byte[] serialize(final Job job) throws Exception {
    final ByteArrayOutputStream bout = new ByteArrayOutputStream();
    final DataOutputStream out = new DataOutputStream(bout);
    try {
      job.write(out);
    } finally {
      out.close();
    }
    return bout.toByteArray();
  }

  Job deserialize(final byte[] bytes) throws Exception {
    final ByteArrayInputStream bin = 
      new ByteArrayInputStream(bytes);
    final DataInputStream in = new DataInputStream(bin);
    final Job job = new Job();
    try {
      job.readFields(in);
    } finally {
      in.close();
    }
    return job;
  }
  
  public Job createJob(final Job old, final BSPJobID jobId, 
                       final HamaConfiguration conf, final Job.State state,
                       final long startTime, final long finishTime) 
      throws Exception {

    final Job.Builder builder = (null == old)? new Job.Builder(): 
                                               new Job.Builder(old);
    return builder.setId(jobId)
                  .setConf(conf)
                  .setName("test-job-serilaization")
                  .setUser("jeremy")
                  .addLocalJobFile("/path/to/job.xml")
                  .setLocalJarFile("/path/to/job.jar")
                  .setNumBSPTasks(numBSPTasks)
                  .setMaster("192.168.2.101")
                  .setMaxTaskAttempts(maxTaskAttempts)
                  .setInputPath("hdfs:///home/user/input")
                  .setState(state)
                  .setProgress(34L)
                  .setSetupProgress(100L)
                  .setCleanupProgress(0L)
                  .setStartTime(startTime)
                  .setFinishTime(finishTime)
                  .setSuperstepCount(1947L)
                  .withTaskTable()
                  .build();
  }

  public void testAttributeOverriden() throws Exception {
    final BSPJobID jobId = createJobId("test", 2);
    final Job.State state = Job.State.RUNNING;
    final long startTime = System.currentTimeMillis();
    final Job old = createJob(null, jobId, conf, state, startTime, -1);
    assertNotNull("Job can't be null!", old);
    final Job.State newState = Job.State.SUCCEEDED;
    final long finishTime = startTime + (1000*60*60*8);
    final Job newJob = createJob(old, jobId, conf, newState, startTime, 
                                 finishTime);
    assertNotNull("New job can't be null!", newJob);
    assertEquals("Finish time should be "+finishTime, 
                 finishTime, newJob.getFinishTime());
    assertEquals("State should be successed!", newState, newJob.getState());
  }

  public void testSerialization() throws Exception {
    final BSPJobID jobId = createJobId("test", 7); 
    final Job.State state = Job.State.RUNNING;
    final long startTime = System.currentTimeMillis();
    final long finishTime = -1;
    
    final Job job = createJob(null, jobId, conf, state, startTime, finishTime);
    final byte[] bytes = serialize(job);
    assertNotNull("Job byte array can't be null.", bytes);
    final Job forVerification = deserialize(bytes);
    assertNotNull("Deserialized job can't be null.", forVerification);
  
    LOG.info("Restored BSPJobID is "+forVerification.getId());
    assertEquals("BSPJobID should be the same.", 
                 forVerification.getId(), jobId);

    LOG.info("Restored job name is "+forVerification.getName());
    assertEquals("Job name should be the same.", 
                 forVerification.getName(), "test-job-serilaization");

    LOG.info("Restored user is "+forVerification.getUser());
    assertEquals("User should be the same.", 
                 forVerification.getUser(), "jeremy");
    
    LOG.info("Restored local jar file is "+forVerification.getLocalJarFile());
    assertEquals("Local jar file should be the same.", 
                 forVerification.getLocalJarFile(), 
                 "/path/to/job.jar");

    LOG.info("Restored numBSPTasks value is "+forVerification.getNumBSPTasks());
    assertEquals("NumBSPTasks should be the same.", 
                 forVerification.getNumBSPTasks(), numBSPTasks);

    LOG.info("Restored master value is "+forVerification.getMaster());
    assertEquals("Master should be the same.", 
                 forVerification.getMaster(), "192.168.2.101");

    LOG.info("MaxAttemptTasks is "+forVerification.getMaxTaskAttempts());
    assertEquals("MaxAttemptTasks should be the same.", 
                 forVerification.getMaxTaskAttempts(), maxTaskAttempts);

    LOG.info("InputPath is "+forVerification.getInputPath());
    assertEquals("InputPath should be the same.", 
                 forVerification.getInputPath(), "hdfs:///home/user/input");

    LOG.info("Restored state is "+forVerification.getState());
    assertEquals("Job State should be equal.", state,
                 forVerification.getState()); 

    LOG.info("Restored progress is "+forVerification.getProgress());
    assertEquals("Progress should be equal.", 34L,
                 forVerification.getProgress()); 

    LOG.info("Restored setupProgress is "+forVerification.getSetupProgress());
    assertEquals("Setup progress should be equal.", 100L,
                 forVerification.getSetupProgress()); 

    LOG.info("CleanupProgress is "+forVerification.getCleanupProgress());
    assertEquals("Cleanup progress should be equal.", 0L,
                 forVerification.getCleanupProgress()); 

    LOG.info("Restored startTime is "+forVerification.getStartTime());
    assertEquals("Start time should be "+startTime, startTime,
                 forVerification.getStartTime());

    LOG.info("Restored finishTime is "+forVerification.getFinishTime());
    assertEquals("Finish time should be "+finishTime, finishTime, 
                 forVerification.getFinishTime());

    LOG.info("SuperstepCount is "+forVerification.getSuperstepCount());
    assertEquals("SuperstepCount should be equal.", 1947L,
                 forVerification.getSuperstepCount()); 

    final TaskTable taskTable = forVerification.table();
    assertNotNull("TaskTable shouldn't be null.", taskTable);
    final Task task = taskTable.get(0, 0);
    assertNotNull("Task shouldn't be null.", task);
    LOG.info("TotalBSPTasks is "+task.getTotalBSPTasks());
    assertEquals("TotalBSPTasks should be equal to numBSPTasks", 
                 task.getTotalBSPTasks(), numBSPTasks);

  }
}
