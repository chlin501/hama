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
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public final class GroomServerStatus implements Writable {
  public static final Log LOG = LogFactory.getLog(GroomServerStatus.class);

  private String name;
  private String host;
  private List<Task> taskReports;
  private int maxTasks;

  public GroomServerStatus(final String name, final String host,  
                           final int maxTasks, final List<Task> taskReports) {
    this.name = name;
    if(StringUtils.isBlank(this.name))
      throw new IllegalArgumentException("GroomServer name is not provided.");
    this.host = host;
    if(StringUtils.isBlank(this.host))
      throw new IllegalArgumentException("GroomServer host is not provided.");
    this.maxTasks = maxTasks;
    if(0 >= this.maxTasks)
      throw new IllegalArgumentException("Invalid max tasks!");
    this.taskReports = new ArrayList<Task>(taskReports);
    if(null == taskReports || this.taskReports.isEmpty())
      throw new IllegalArgumentException("Tasks list is empty!");
  }

  public String name() {
    return name;
  }

  public String host() {
    return host;
  }
  
  public List<Task> taskReports() {
    return taskReports;
  }

  public int maxTasks() {
    return maxTasks;
  }
  
  @Override
  public int hashCode() {
    int result = 17;
    result = 37 * result + name.hashCode();
    result = 37 * result + host.hashCode();
    return result;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this)
      return true;
    if (null == o)
      return false;
    if (getClass() != o.getClass())
      return false;

    GroomServerStatus s = (GroomServerStatus) o;
    if (!s.name.equals(name))
      return false;
    return true;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.name = Text.readString(in);
    this.host = Text.readString(in);

    this.maxTasks = in.readInt();
    taskReports.clear();
    int numTasks = in.readInt();

    Task task;
    for (int i = 0; i < numTasks; i++) {
      task = new Task();
      task.readFields(in);
      taskReports.add(task);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, name);
    Text.writeString(out, host);

    out.writeInt(maxTasks);
    out.writeInt(taskReports.size());
    for (final Task task: taskReports) {
      task.write(out);
    }
  }

}