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

import org.apache.hama.bsp.BSPJobID;
import org.apache.hama.bsp.TaskID;
import org.apache.hama.bsp.TaskAttemptID;

public final class IDCreator {

  public static final class BSPJobIDBuilder {
    private String jtIdentifier; 
    private int id; 

    public BSPJobIDBuilder withId(final String jtIdentifier) {
      this.jtIdentifier = jtIdentifier;
      return this;
    }
    
    public BSPJobIDBuilder withId(final int id) {
      this.id = id;
      return this;
    }

    public TaskIDBuilder getTaskIDBuilder() {
      final TaskIDBuilder taskIdBuilder = new TaskIDBuilder();
      taskIdBuilder.withId(build());
      return taskIdBuilder;
    }
    
    public BSPJobID build() {
      return new BSPJobID(jtIdentifier, id);
    }
  }

  public static final class TaskIDBuilder {
    private BSPJobID jobId; 
    private int id; 

    public TaskIDBuilder withId(final BSPJobID jobId) {
      this.jobId = jobId;
      return this;
    }

    public TaskIDBuilder withId(final int id) {
      this.id = id;
      return this;
    }

    public TaskAttemptIDBuilder getTaskAttemptIDBuilder() {
      final TaskAttemptIDBuilder taskAttemptIdBuilder = 
        new TaskAttemptIDBuilder();
      taskAttemptIdBuilder.withId(build());
      return taskAttemptIdBuilder;
    }

    public TaskID build() {
      return new TaskID(jobId, id);
    }
  }

  public static final class TaskAttemptIDBuilder {
    private TaskID taskId; 
    private int id;  

    public TaskAttemptIDBuilder withId(final TaskID taskId) {
      this.taskId = taskId;
      return this;
    }

    public TaskAttemptIDBuilder withId(final int id) {
      this.id = id;
      return this;
    }

    public TaskAttemptID build() {
      return new TaskAttemptID(taskId, id);
    }
  }

  public static BSPJobIDBuilder newBSPJobID() {
    return new BSPJobIDBuilder();
  }

  public static TaskIDBuilder newTaskID() {
    return new TaskIDBuilder();
  }

  public static TaskAttemptIDBuilder newTaskAttemptID() {
    return new TaskAttemptIDBuilder();
  }
}
