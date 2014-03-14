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

public final class GroomServerSpec implements Writable {

  public static final Log LOG = LogFactory.getLog(GroomServerSpec.class);

  private String name; // groom_<host>_<port>
  private String host; // groom (rpc) host addr default 0.0.0.0
  private int port;
  private int maxTasks;

  public GroomServerSpec(final String name, final String host, final int port,
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
      throw new IllegalArgumentException("Invalid max tasks!");
  }

  public String name() {
    return name;
  }

  public String host() {
    return host;
  }

  public int port() {
    return this.port;
  }

  public int maxTasks() {
    return maxTasks;
  }
  
  @Override
  public int hashCode() {
    int result = 17;
    result = 37 * result + name.hashCode();
    result = 37 * result + host.hashCode();
    result = 37 * result + new Integer(port).hashCode(); // TODO
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

    GroomServerSpec s = (GroomServerSpec) o;
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
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, name);
    Text.writeString(out, host);
    out.writeInt(port);
    out.writeInt(maxTasks);
  }

}
