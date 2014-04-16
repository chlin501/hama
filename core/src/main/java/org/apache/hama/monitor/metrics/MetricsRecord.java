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
package org.apache.hama.monitor.metrics;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.Collection;
import java.util.Collections;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Represents a record containing multiple metrics.
 */
public final class MetricsRecord implements Writable {

  private String groomName;
  private String name, description;
  private List<Metric> metrics = new ArrayList<Metric>();

  public MetricsRecord() { 
    this("groom_0.0.0.0_50000", "default");
  }

  public MetricsRecord(String groomName, String name) {
    this(groomName, name, name + " record.");
  }

  public MetricsRecord(String groomName, String name, String description) {
    this.groomName = groomName;
    this.name = name;
    this.description = description;
  }

  public final String getGroomName() {
    return groomName;
  }

  public final String getName() {
    return name;
  }

  public final String getDescription() {
    return description;
  }

  public final void add(Metric metric) {
    metrics.add(metric);
  }

  public final void add(List<Metric> metrics) {
    this.metrics.addAll(metrics);
  }

  public final List<Metric> getMetrics() {
    return Collections.unmodifiableList(metrics);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, this.groomName);
    Text.writeString(out, this.name);
    Text.writeString(out, this.description);
    out.writeInt(metrics.size());
    for (Metric metric : metrics) {
      metric.write(out);
    }
  }
       
  @Override
  public void readFields(DataInput in) throws IOException {
    this.groomName = Text.readString(in);
    this.name = Text.readString(in);
    this.description = Text.readString(in);
    metrics.clear();
    final int numOfRecords = in.readInt();
    for (int i = 0; i < numOfRecords; i++) {
      final Metric metric = new Metric();
      metric.readFields(in);
      metrics.add(metric);
    }
  }

  @Override
  public String toString() {
    return "groomName:" + groomName + " name: " + name + " description: " +
           description; 
  }

  @Override 
  public boolean equals(Object obj) {
    if (obj == null) { return false; }
    if (obj == this) { return true; }
    if (obj.getClass() != getClass()) {
      return false;
    }

    final MetricsRecord m = (MetricsRecord) obj;
    if (!getGroomName().equals(m.groomName))
      return false;
    if (!getName().equals(m.name))
      return false;
    if (!getDescription().equals(m.description))
      return false;

    return true;
    
  }

  @Override 
  public int hashCode() {
    int result = 17;
    result = 37 * result + groomName.hashCode(); 
    result = 37 * result + name.hashCode(); 
    result = 37 * result + description.hashCode();
    return result;
  }
  
}