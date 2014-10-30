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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * The latest GroomServer statistic data.
 * TaskManager will report to zk and request for task with this stat data.
 */
//TODO: move to groom.
public final class GroomServerStat implements Writable {

  public static final Log LOG = LogFactory.getLog(GroomServerStat.class);

  /* The name of the GroomServer in the form of groom_<host>_<port> */
  private String name;

  /* The GroomServer host address; default to 0.0.0.0 */
  private String host; 

  /* The port value used by the GroomServer. */
  private int port;

  /* Hard bound for max tasks a GroomServer can have. */
  private int maxTasks;

  /* queue that contains tasks. */
  private ArrayWritable queue = new ArrayWritable(Text.class);

  /* slot usage to execute tasks. */
  private ArrayWritable slots = new ArrayWritable(Text.class); 

  public GroomServerStat() { } // for Serializer used only 

  /**
   * GroomServer stat at this moment. 
   * @param name of this GroomServer in a form of groom_<host>_<port>.
   * @param host of this GroomServer.
   * @param port of this GroomServer.
   * @param maxTasks details the max tasks this GroomServer can hold.
   */
  public GroomServerStat(final String name, final String host, final int port,
                         final int maxTasks) {
    this.name = name;
    if(StringUtils.isBlank(this.name))
      throw new IllegalArgumentException("GroomServer name is not provided.");

    this.host = host;
    if(StringUtils.isBlank(this.host))
      throw new IllegalArgumentException("GroomServer host is not provided.");

    this.port = port;
    if(0 > this.port)
      throw new IllegalArgumentException("Invalid port value!");

    this.maxTasks = maxTasks;
    if(0 >= this.maxTasks)
      throw new IllegalArgumentException("Invalid max tasks: "+maxTasks+"!");

    // initialize the queue, otherwise NPE is thrown.
    this.queue.set(new Text[]{}); 

    final Text[] groomServers = new Text[this.maxTasks];
    for(int pos=0; pos< groomServers.length; pos++) {
      groomServers[pos] = new Text(toNullString()); // (null)
    } 
    this.slots.set(groomServers);
  }

  public static final String toNullString() {
    return NullWritable.get().toString();
  }

  public static final boolean isNullString(final String string) {
    return (toNullString().equals(string));
  }

  /**
   * The name of the GroomServer.
   * @return String in a form of groom_<host>_<port>
   */
  public String getName() {
    return name;
  }

  /**
   * The host of the GroomServer.
   * @return String vlaue is the GroomServer host.
   */
  public String getHost() {
    return host;
  }

  /**
   * The port used by the GroomServer.
   * @return int denotes the port number.
   */
  public int getPort() {
    return this.port;
  }

  /**
   * The max tasks the GroomServer can manage.
   * @return int denotes the value of the maxTasks.
   */
  public int getMaxTasks() {
    return maxTasks;
  }

  /**
   * Jobs hold within GroomServer's queue. 
   * TODO: remove this one because this value should always be 0 as GroomServer deals 1, slots 2, queue, 3 request orderly. So no request to sched if slots or queue is not empty.
   * @return 
   */
  public int queueLength() {
    return this.queue.get().length;
  }

  public void addNullToQueue() {
    addToQueue(toNullString());
  }

  /**
   * Add a taskAttemptId to the end of a queue.
   */
  public void addToQueue(final String taskAttemptId) {
    final Writable[] taskAttemptIds = this.queue.get();   
    final Writable[] tmp = new Writable[queueLength()+1];
    System.arraycopy(taskAttemptIds, 0, tmp, 0, queueLength());
    tmp[tmp.length-1] = new Text(taskAttemptId);
    this.queue.set(tmp);
  }

  public void setQueue(final String[] taskAttemptIds) {
    this.queue.set(toTextArray(taskAttemptIds));
  }

  public String[] getQueue() {
    return this.queue.toStrings();
  }

  Text[] toTextArray(final String[] strings) {
    final Text[] tmp = new Text[strings.length];
    for(int pos=0; pos < tmp.length; pos++) 
      tmp[pos] = new Text(strings[pos]);
    return tmp;
  }

  public void markWithNull(final int idx) {
    mark(idx, toNullString());
  }

  public int slotsLength() {
    return this.slots.get().length;
  }

  /**
   * Mark a specific slot as assigned to the taskAttemptId.
   * @param idx is the index in the slots array. 
   * @param taskAttemptId is the name that identifies a GroomServer.
   */
  public void mark(final int idx, final String taskAttemptId) {
    final Writable[] taskAttemptIds = slots.get();
    if(0 > idx) 
      throw new IllegalArgumentException("Invalid array index "+idx);
    if(idx >= slotsLength()) {
      throw new IllegalArgumentException("Can't mark the "+idx+"-th slot "+
                                         "for idx >= "+slotsLength());
    }
    taskAttemptIds[idx] = new Text(taskAttemptId);
  }

  /**
   * Mark the entire slots with the GroomServers specified.
   * @param slotsUsage denotes the slots occupied by GroomServers.
   */
  public void mark(final Writable[] slotsUsage) {
    if(slotsLength() != slotsUsage.length) 
      throw new IllegalArgumentException("Specified slotsUsage length "+
                                         slotsUsage.length+" not match slots"+
                                         " allocated "+slotsLength());
    slots.set(slotsUsage);
  }

  /**
   * Obtain the slots used by GroomServers.
   * @return String[] is an array of slots occupied by GroomServers.
   */
  public String[] getSlots() {
    return slots.toStrings();
  }
  
  /**
   * We only need to identify if it's the same GroomServer.
   */
  @Override
  public int hashCode() {
    int result = 17;
    result = 37 * result + name.hashCode();
    result = 37 * result + host.hashCode();
    result = 37 * result + new Integer(port).hashCode(); 
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

    GroomServerStat s = (GroomServerStat) o;
    if (!s.name.equals(name))
      return false;
    return true;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.name = Text.readString(in);
    this.host = Text.readString(in);
    this.port = in.readInt();
    this.maxTasks = in.readInt();
    this.queue = new ArrayWritable(Text.class);
    this.queue.set(new Text[]{}); // init queue
    this.queue.readFields(in);
    // init slots
    this.slots = new ArrayWritable(Text.class);
    final Text[] groomServers = new Text[this.maxTasks];
    for(int pos=0; pos< groomServers.length; pos++) {
      groomServers[pos] = new Text(toNullString()); 
    }
    this.slots.set(groomServers);
    this.slots.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, name);
    Text.writeString(out, host);
    out.writeInt(port);
    out.writeInt(maxTasks);
    this.queue.write(out);
    this.slots.write(out);
  }
}
