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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.Text;
import org.apache.hama.bsp.v2.Task;
import static org.apache.hama.master.Directive.*;
import static org.apache.hama.master.Directive.Action.*;

/**
 * A command instructs {@link GroomServer}'s execution.
 */
public class Directive implements Writable {

  public static final Log LOG = LogFactory.getLog(Directive.class);

  /**
   * The timestamp when this directed is created. 
   */
  protected long timestamp;

  /**
   * Tell which master posts the request. 
   */
  protected String master; 

  protected Action action;

  protected Task task;

  public static enum Action {
    Launch(1), 
    Kill(2),  // TODO; rename to cancel?
    Resume(3); 

    int t;
    Action(int t) { this.t = t; }
    public int value() { return this.t; }
  }

  public Directive(){ } // for writable
  
  public Directive(final Action action, final Task task, final String master) {
    this.timestamp = System.currentTimeMillis();

    this.master = master;
    if(null == this.master)
      throw new IllegalArgumentException("Master is not assigned.");

    this.action = action;
    if(null == this.action)
      throw new IllegalArgumentException("No action is provided.");

    this.task = task;
    if(null == this.task)
      throw new IllegalArgumentException("No task is provided.");
  }

  public long timestamp() {
    return this.timestamp;
  }

  public String master() {
    return this.master;
  }

  public Action action() {
    return this.action;
  }

  public Task task() {
    return this.task;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) return true;
    if (null == o) return false;
    if (getClass() != o.getClass()) return false;

    final Directive d = (Directive) o;
    if (!d.task.equals(task)) return false;
    if (!d.master.equals(master)) return false;
    if (!d.action.toString().equals(action.toString())) return false;
    return true;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeLong(timestamp());
    Text.writeString(out, master());
    out.writeInt(action().value());
    this.task.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.timestamp = in.readLong();
    this.master = Text.readString(in);
    int t = in.readInt();
    if (Launch.value() == t) {
      this.action = Launch;
    } else if(Kill.value() == t) {
      this.action = Kill;
    } else if(Resume.value() == t) {
      this.action = Resume;
    } else {
      LOG.error("Unknown action value: "+t);
    }
    this.task = new Task();
    this.task.readFields(in);
  }

  @Override 
  public String toString() {
    return "Directive("+action.toString() + "," +
                        master + "," +
                        timestamp + "," + 
                        task.toString()+")";
  }
}
