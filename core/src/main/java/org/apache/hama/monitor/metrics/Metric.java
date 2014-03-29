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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;


public final class Metric implements Writable {

  private String name;
  private String description;
  private Class<? extends Writable> declared;
  private Writable value;

  public Metric() { }

  public Metric(Enum<?> name, Writable value) {
    this(name.name(), name.name() + " metric.", 
         value.getClass(), value);
  }

  public Metric(String name, String description, 
                Class<? extends Writable> declared, Writable value) {
    this.name = name;
    if(null == this.name) 
      throw new IllegalArgumentException("Metric name is not provided!");

    this.description = description;
    if(null == this.description) 
      throw new IllegalArgumentException("Metric description is not found!");

    this.declared = declared;
    if(null == this.declared) 
      throw new IllegalArgumentException("Declared class is not provided!");

    this.value = value;
    if(null == this.value) 
      throw new IllegalArgumentException("Metric value is not provided!");
  }

  public final String getName() {
    return this.name;
  }

  public final String getDescription() {
    return this.description;
  }

  public final Class<? extends Writable> getDeclared() {
    return this.declared;
  }

  public final Writable getValue() {
    return this.value;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, this.name);
    Text.writeString(out, this.description);
    Text.writeString(out, declared.getName()); 
    value.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.name = Text.readString(in);
    this.description = Text.readString(in);
    String className = Text.readString(in);
    if(IntWritable.class.getName().equals(className)) {
      this.declared = IntWritable.class;
      this.value = new IntWritable();
      this.value.readFields(in);
    } else if(LongWritable.class.getName().equals(className)) {
      this.declared = LongWritable.class;
      this.value = new LongWritable();
      this.value.readFields(in);
    } else {
      throw new RuntimeException(this.declared+" class not supported.");
    }
  }

  @Override
  public boolean equals(Object target) {
    if (target == this)
      return true;
    if (null == target)
      return false;
    if (getClass() != target.getClass())
      return false;

    Metric m = (Metric) target;
    if (!getName().equals(m.name))
      return false;
    if (!getDescription().equals(m.description))
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = 17;
    result = 37 * result + name.hashCode();
    result = 37 * result + description.hashCode();
    return result;
  }

  @Override 
  public String toString() {
    final StringBuilder builder = new StringBuilder();
    builder.append("name: " + name +" description: "+ description+" value: ");
    if(declared.equals(IntWritable.class)) {
      builder.append(((IntWritable)value).get());
    } else if(declared.equals(LongWritable.class)) {
      builder.append(((LongWritable)value).get());
    } else {
      builder.append(value);
    }
    return builder.toString();
  }

}
