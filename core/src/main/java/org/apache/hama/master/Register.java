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
package org.apache.hama.master;
/**
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

 * An object for identify GroomServer basic information, including 
 * GroomServer's name and its max tasks allowed to run.
public final class Register implements Writable {
 
  private Text groomServerName = new Text("");  
  private IntWritable maxTasks = new IntWritable(3); 

   * Information when a Groomserver registers to the Master server.
   * @param groomName identifies the groom server.
   * @param maxTasks denotes the max tasks allowed to run on the groom server.
  public Register(final String groomName, final int maxTasks) {
    if(null == groomName || groomName.isEmpty())
      throw new IllegalArgumentException("GroomName is not provided.");
    if(0 >= maxTasks)
      throw new IllegalArgumentException("Invalid maxTasks value.");
    this.groomServerName = new Text(groomName);
    this.maxTasks = new IntWritable(maxTasks);
  }

  public String getGroomServerName() {
    return groomServerName.toString();
  }

  public int getMaxTasks() {
    return maxTasks.get();
  }

  @Override
  public int hashCode() {
    int result = 17;
    result = 37 * result + groomServerName.toString().hashCode();
    result = 37 * result + new Integer(maxTasks.get()).hashCode();
    return result;
  }

   * MaxTasks may be updated so we only use GroomServerName for identification.
   * @param o for object to be compared.
   * @return boolean identifies if it's the same object when true; otherwise
   *                 false when object is not the same.
  @Override
  public boolean equals(Object o) {
    if (o == this)
      return true;
    if (null == o)
      return false;
    if (getClass() != o.getClass())
      return false;

    Register s = (Register) o;
    if (!s.groomServerName.equals(groomServerName))
      return false;
    return true;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    groomServerName.write(out);
    maxTasks.write(out);
  }

  @Override 
  public void readFields(DataInput in) throws IOException {
    groomServerName = new Text("");
    groomServerName.readFields(in);
    maxTasks = new IntWritable(3);
    maxTasks.readFields(in);
  }
}
*/
