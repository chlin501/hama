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

public class SystemInfo implements Writable {

  private Text protocol = new Text(Protocol.Local.toString());
  private Text actorSystemName = new Text();
  private Text host = new Text();
  private IntWritable port = new IntWritable();

  static enum Protocol {
    Local("akka"), Remote("akka.tcp");
    final String p;
    Protocol(final String v) {
      this.p = v;
    }
    public String toString() { return this.p; }
  }

  public SystemInfo(final String actorSystemName,
                    final String host,
                    final int port) {
    this(Protocol.Local, actorSystemName, host, port);
  }

  public SystemInfo(final Protocol protocol,
                    final String actorSystemName,
                    final String host,
                    final int port) {
    if(null == protocol)
      throw new IllegalArgumentException("Protocol is missing!");
    this.protocol = new Text(protocol.toString());

    if(null == actorSystemName)
      throw new IllegalArgumentException("Actor system name not provided.");

    this.actorSystemName = new Text(actorSystemName); 

    if(null == host)
      throw new IllegalArgumentException("Target host is not provided.");

    this.host = new Text(host);

    if(0 > port)
      throw new IllegalArgumentException("Illegal port value.");

    this.port = new IntWritable(port);
  }

  public Protocol getProtocol() {
    Protocol p = Protocol.Local;
    if(this.protocol.toString().equals(Protocol.Remote.toString())) {
      p = Protocol.Remote;
    }
    return p; 
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
