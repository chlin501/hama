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
package org.apache.hama;

import java.io.IOException;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

// TODO: rename to Node?
public class SystemInfo implements Writable {

  public static final String Localhost = "127.0.0.1";
  public static final int LocalMode = -1;

  protected Text protocol = new Text(Protocol.Remote.toString());
  protected Text actorSystemName = new Text();
  protected Text host = new Text();
  protected IntWritable port = new IntWritable();

  public static enum Protocol { 
    Local("akka"), Remote("akka.tcp");
    final String p;
    Protocol(final String v) {
      this.p = v;
    }
    public String toString() { return this.p; }
  }

  /**
   * For local lookup.
   */
  public SystemInfo(final String actorSystemName) {
    this(Protocol.Local.toString(), actorSystemName, Localhost, LocalMode);
  }

  /**
   * For remote lookup.
   */
  public SystemInfo(final String actorSystemName,
                    final String host,
                    final int port) {
    this(Protocol.Remote.toString(), actorSystemName, host, port);
  }
 
  public SystemInfo(final String protocol,
                    final String actorSystemName,
                    final String host,
                    final int port) {
    if(null == protocol || protocol.isEmpty())
      throw new IllegalArgumentException("Protocol is missing!");
    this.protocol = new Text(protocol);

    if(null == actorSystemName)
      throw new IllegalArgumentException("Actor system name not provided.");

    this.actorSystemName = new Text(actorSystemName); 

    if(null == host)
      throw new IllegalArgumentException("Target host is not provided.");

    this.host = new Text(host);
  
    if(port > 65535) // port -1 might denote local
      throw new IllegalArgumentException("Invalid port value! port: "+port);

    this.port = new IntWritable(port);
  }

  public Protocol getProtocol() {
    Protocol p = Protocol.Remote;
    if(isLocal()) {
      p = Protocol.Local;
    }
    return p; 
  }

  public boolean isLocal() {
    return Protocol.Local.toString().equals(protocol.toString());
  }
  
  public String getActorSystemName() {
    return this.actorSystemName.toString();
  }

  public String getHost() {
    return this.host.toString();
  }

  public int getPort() {
    return this.port.get();
  }

  public String getAddress() {
    if(isLocal()) {
      return getActorSystemName();
    } else {
      return getActorSystemName() + "@" + getHost() + ":" + getPort();
    }
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) return true;
    if (null == o) return false;
    if (getClass() != o.getClass()) return false;
    final SystemInfo s = (SystemInfo) o;
    if (!s.protocol.toString().equals(protocol.toString())) return false;
    if (!s.actorSystemName.toString().equals(actorSystemName.toString())) 
      return false;
    if (!s.host.toString().equals(host.toString())) return false;
    if (s.port.get() != port.get()) return false;
    return true;
  }

  @Override
  public int hashCode() {
    int result = 17;
    result = 37 * result + protocol.toString().hashCode();
    result = 37 * result + actorSystemName.toString().hashCode();
    result = 37 * result + host.toString().hashCode();
    result = 37 * result + port.get();
    return result;
  }

  @Override 
  public String toString() {
    return "SystemInfo("+getProtocol()+","+
                         getActorSystemName()+","+
                         getHost()+","+
                         getPort()+")";
  }

  @Override 
  public void write(DataOutput out) throws IOException {
    this.protocol.write(out);     
    this.actorSystemName.write(out);     
    this.host.write(out);
    this.port.write(out);
  }

  @Override 
  public void readFields(DataInput in) throws IOException {
    this.protocol.readFields(in);
    this.actorSystemName.readFields(in);
    this.host.readFields(in);
    this.port.readFields(in);
  }
}
