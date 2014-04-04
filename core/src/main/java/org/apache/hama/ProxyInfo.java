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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public final class ProxyInfo extends SystemInfo implements Writable {

  private Text actorName = new Text();
  private Text actorPath = new Text();

  public final static class Builder {
    private String actorName;
    private String actorSystemName;
    private String host;
    private int port;
    private StringBuilder actorPath = new StringBuilder();
    private HamaConfiguration conf;

    public Builder withActorName(final String actorName) {
      this.actorName = actorName;      
      return this;
    }

    public Builder withActorSystemName(final String actorSystemName) {
      this.actorSystemName = actorSystemName;      
      return this;
    }

    public Builder withHost(final String host) {
      this.host = host;      
      return this;
    }

    public Builder withPort(final int port) {
      this.port = port;      
      return this;
    }

    private void assertPath(final String fullPath) {
      if(null == fullPath || fullPath.isEmpty() || fullPath.endsWith("/"))
        throw new IllegalArgumentException("Invalid actorPath! "+fullPath);
    }

    public Builder withActorPath(final String fullPath) {
      assertPath(fullPath);
      this.actorPath.append(actorPath);
      return this;
    }

    public Builder appendRootPath(final String root) {
      this.actorPath.append(root);
      return this;
    }

    public Builder appendChildPath(final String child) {
      if(0 >= this.actorPath.length()) 
        throw new RuntimeException("Root actor path is missing!");
      this.actorPath.append("/"+child);
      return this;
    }

    public Builder withConfiguration(final HamaConfiguration conf) {
      this.conf = conf;
      return this;
    }
   
    public ProxyInfo build() {
      return new ProxyInfo(this.actorName, this.actorSystemName, this.host, 
                           this.port, this.actorPath.toString());
    } 

    public ProxyInfo buildProxyAtMaster() {
      if(null == this.conf) 
        throw new NullPointerException("HamaConfiguration not found.");
      final String sysName = this.conf.get("bsp.master.actor-system.name", 
                                           "MasterSystem");
      final String hostV = this.conf.get("bsp.master.address", "127.0.0.1");
      final int portV = this.conf.getInt("bsp.master.port", 40000);
      final String fullPath = this.actorPath.toString();
      assertPath(fullPath);
      return new ProxyInfo(this.actorName, sysName, hostV, portV, fullPath);
    }

    public ProxyInfo buildProxyAtGroom() {
      if(null == this.conf) 
        throw new NullPointerException("HamaConfiguration not found.");
      final String sysName = this.conf.get("bsp.groom.actor-system.name", 
                                           "GroomSystem");
      final String hostV = this.conf.get("bsp.groom.address", "127.0.0.1");
      final int portV = this.conf.getInt("bsp.groom.port", 50000);
      final String fullPath = this.actorPath.toString();
      assertPath(fullPath);
      return new ProxyInfo(this.actorName, sysName, hostV, portV, fullPath); 
    }
  }
  
  public ProxyInfo(final String actorName,
                   final String actorSystemName, 
                   final String host,
                   final int port, 
                   final String actorPath) {

    super(actorSystemName, host, port);

    if(null == actorName) 
      throw new IllegalArgumentException("Actor name not provided.");

    this.actorName = new Text(actorName);

    if(null == actorPath) 
      throw new IllegalArgumentException("Actor path not provided.");
  }

  public String getActorName() {
    return this.actorName.toString();
  }
  
  public String getPath() {
    final String path = "akka.tcp://"+getActorSystemName()+"@"+getHost()+":"+
                        getPort()+"/user/"+getActorPath();
    return path;
  }

  public String getActorPath() {
    return this.actorPath.toString();
  }

  @Override 
  public void write(DataOutput out) throws IOException {
    super.write(out);
    this.actorName.write(out);
    this.actorPath.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    super.readFields(in);
    this.actorName.readFields(in);
    this.actorPath.readFields(in);
  }
}
