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

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hama.bsp.BSPJobID;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.bsp.TaskID;

public class TestIDCreator extends TestCase {

  final Log LOG = LogFactory.getLog(TestIDCreator.class);

  public void testBuilder() throws Exception {
    final BSPJobID jobId = IDCreator.newBSPJobID()
                                    .with("jtIdentifer")
                                    .with(1)
                                    .build();
    assertEquals("BSPJobID jtIdentifer should equal.", 
                 jobId.getJtIdentifier(), "jtIdentifer"); 
    assertEquals("BSPJobID ", 
                 jobId.getId(), 1); 

    final TaskID taskId = IDCreator.newBSPJobID()
                                   .with("jtIdentifer")
                                   .with(1)
                                   .getTaskIDBuilder()
                                   .with(2)
                                   .build();
    assertEquals("TaskID's BSPJob should equal.", 
                 jobId.toString(), taskId.getJobID().toString()); 
    assertEquals("TaskID's id should equal.", 2, taskId.getId()); 

    final TaskAttemptID attemptId = IDCreator.newBSPJobID()
                                             .with("jtIdentifer")
                                             .with(1)
                                             .getTaskIDBuilder()
                                             .with(2)
                                             .getTaskAttemptIDBuilder()
                                             .with(3)
                                             .build();
    assertEquals("TaskAttemptID's TaskID should equal.", 
                 taskId.toString(), attemptId.getTaskID().toString()); 
    assertEquals("TaskAttemptID's id should equal.", 3, attemptId.getId()); 

    final TaskID taskId1 = IDCreator.newTaskID().with(jobId).with(7).build();
    assertEquals("TaskID's BSPJobID should equal.", 
                 jobId.toString(), taskId.getJobID().toString()); 
    assertEquals("TaskID's id should equal.", 7, taskId1.getId());

    final TaskAttemptID attemptId1 = 
      IDCreator.newTaskAttemptID().with(taskId1).with(11).build();
    assertEquals("TaskAttemptID's TaskID should equal.", 
                 taskId1.toString(), attemptId1.getTaskID().toString()); 
    assertEquals("TaskAttemptID's id should equal.", 11, attemptId1.getId());
    
  }


}
