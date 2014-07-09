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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Actor location information, including
 * - Protocol 
 * - ActorSystemName
 * - Host
 * - Port
 * - ActorPath
 * This class will provide Builder, MasterBuilder and GroomBuilder, for 
 * constructing the proxy information.
 * 
 * TODO: 1. group with ActorLocator
 */
public final class ProxyInfo extends SystemInfo implements Writable {

  public static final Log LOG = LogFactory.getLog(ProxyInfo.class);

  private Text actorName = new Text();
  private Text actorPath = new Text();

  public final static class MasterBuilder extends Builder {
    
    public MasterBuilder(final String actorName, final HamaConfiguration conf) {
      super.protocol = Protocol.Remote;
      if(null == actorName)
        throw new IllegalArgumentException("Master actor name is missing!");
      super.actorName = actorName;

      if(null == conf) 
        throw new IllegalArgumentException("HamaConfiguration is missing!"); 
      super.conf = conf;

      actorSystemName = conf.get("bsp.master.actor-system.name", 
                                 "MasterSystem");
      host = conf.get("bsp.master.address", "127.0.0.1");
      port = conf.getInt("bsp.master.port", 40000);
      LOG.debug("Master proxy "+actorName+" is at "+host+":"+port+" with path "+
                actorPathBuilder.toString());
    } 

  } 

  public final static class GroomBuilder extends Builder {

    public GroomBuilder(final String actorName, final HamaConfiguration conf) {
      super.protocol = Protocol.Remote;
      if(null == actorName || actorName.isEmpty())
        throw new IllegalArgumentException("Groom actor name is missing!");
      super.actorName = actorName;

      if(null == this.conf) 
        throw new NullPointerException("HamaConfiguration not found.");
      super.conf = conf;

      actorSystemName = conf.get("bsp.groom.actor-system.name", 
                                 "GroomSystem");
      host = conf.get("bsp.groom.address", "127.0.0.1");
      port = conf.getInt("bsp.groom.port", 50000);
      LOG.debug("Groom proxy "+actorName+" is at "+host+":"+port+" with path "+ 
                actorPathBuilder.toString());
    }

  }

  public final static class ActorPathBuilder {

    private final Builder builder;
    private StringBuilder actorPath = new StringBuilder();

    public ActorPathBuilder(final Builder builder) {
      if(null == builder)
        throw new IllegalArgumentException("Builder is missing!");
      this.builder = builder;
    }

    private void assertPath(final String fullPath) {
      if(null == fullPath || fullPath.isEmpty() || fullPath.endsWith("/"))
        throw new IllegalArgumentException("Invalid actorPath! "+fullPath);
    }

    public ActorPathBuilder withActorPath(final String fullPath) {
      assertPath(fullPath);
      this.actorPath = new StringBuilder(actorPath);
      return this;
    }

    public ActorPathBuilder appendRootPath(final String root) {
      if(null == root || root.isEmpty() || root.endsWith("/"))
        throw new IllegalArgumentException("Malformed root path: "+root);
      if(!this.actorPath.toString().isEmpty())
        throw new RuntimeException("Actor path is already defind: "+
                                   this.actorPath.toString());
      this.actorPath.append(root);
      return this;
    }

    public ActorPathBuilder appendChildPath(final String child) {
      if(null == child || child.startsWith("/"))
        throw new IllegalArgumentException("Child actor path can't be null or"+
                                           " started with '/'");
      if(this.actorPath.toString().isEmpty()) 
        throw new RuntimeException("Root actor path is missing!");
      this.actorPath.append("/"+child);
      return this;
    } 

    public String toString() {
      return actorPath.toString();
    }

    public ProxyInfo build() {
      return new ProxyInfo(builder.protocol, builder.actorName, 
                           builder.actorSystemName, builder.host, 
                           builder.port, toString());
    }
  }

  static class Builder {
    HamaConfiguration conf = new HamaConfiguration();
    Protocol protocol = Protocol.Remote; 
    String actorName;
    String actorSystemName;
    String host;
    int port;
    ActorPathBuilder actorPathBuilder = new ActorPathBuilder(this); 

    public Builder withLocalActor() {
      this.protocol = Protocol.Local; 
      return this;
    }

    public Builder withRemoteActor() {
      this.protocol = Protocol.Remote; 
      return this;
    }

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

    public ActorPathBuilder createActorPath() {
      final ActorPathBuilder actorPathBuilder = new ActorPathBuilder(this);
      return actorPathBuilder; 
    }

    public Builder withActorPath(final ActorPathBuilder actorPathBuilder) {
      if(null == actorPathBuilder)
        throw new IllegalArgumentException("Actor path builder is missing!");
      this.actorPathBuilder = actorPathBuilder;
      return this;
    }

    public Builder withConfiguration(final HamaConfiguration conf) {
      if(null == conf)
        throw new IllegalArgumentException("HamaConfiguration is missing!");
      this.conf = conf;
      return this;
    }
   
    public ProxyInfo build() {
      return new ProxyInfo(this.protocol, this.actorName, this.actorSystemName,
                           this.host, this.port, actorPathBuilder.toString());
    } 
  }
  
  public ProxyInfo(final Protocol protocol,
                   final String actorName,
                   final String actorSystemName, 
                   final String host,
                   final int port, 
                   final String actorPath) {
    super(protocol, actorSystemName, host, port);

    if(null == actorName) 
      throw new IllegalArgumentException("Actor name not provided.");

    this.actorName = new Text(actorName);

    if(null == actorPath) 
      throw new IllegalArgumentException("Actor path not provided.");

    this.actorPath = new Text(actorPath);
  }

  public String getActorName() {
    return this.actorName.toString();
  }

  public String getPath() {
    if(Protocol.Local.equals(getProtocol())) {
      return getProtocol()+"://"+getActorSystemName()+"/user/"+getActorPath();
    } else {
      return getProtocol()+"://"+getActorSystemName()+"@"+getHost()+":"+
             getPort()+"/user/"+getActorPath();
    }
  }

  public String getActorPath() {
    return this.actorPath.toString();
  }

  /**
   * Convert proxy address e.g.
   * - remote
   *   akka.tcp://${actor-system-name}@${host}:${port}/user/${actor-path} 
   * - local
   *   akka://${actor-system-name}/user/${actor-path}
   * from String to ProxyInfo object.
   */
  public static ProxyInfo fromString(final String address) {
    final String[] protoWithRest = address.split("://");
    if(2 != protoWithRest.length) 
      throw new RuntimeException("Invalid protocol format: "+address);
    Protocol proto = Protocol.Remote;
    if(!Protocol.Remote.toString().equals(protoWithRest[0])) 
      proto = Protocol.Local;
    if(Protocol.Remote.equals(proto)) {
      final String[] actorSysWithRest = protoWithRest[1].split("@");
      if(2 != actorSysWithRest.length)
        throw new RuntimeException("Invalid actor system name format: "+
                                   address);
      final String actorSysName = actorSysWithRest[0]; 
      final String[] hostWithRest = actorSysWithRest[1].split(":");
      if(2 != hostWithRest.length)
        throw new RuntimeException("Invalid host format: "+address);
      final String host = hostWithRest[0];
      final String[] portAndActorPath = hostWithRest[1].split("/user/");
      if(2 != portAndActorPath.length)
      throw new RuntimeException("Invalid port format: "+address);
      final int port = new Integer(portAndActorPath[0]).intValue();
      final String actorPath = portAndActorPath[1];
      final String[] actors = actorPath.split("/");
      final String actorName = actors[actors.length-1];
      return new ProxyInfo(proto, 
                           actorName,
                           actorSysName,
                           host,
                           port,
                           actorPath);
    } else {
      final String[] actorSysWithRest = protoWithRest[1].split("/user/");
      if(2 != actorSysWithRest.length) 
        throw new RuntimeException("Invalid actor sys format: "+address);
      final String actorSysName = actorSysWithRest[0];
      final String actorPath = actorSysWithRest[1];
      final String[] actors = actorSysWithRest[1].split("/");
      final String actorName = actors[actors.length-1];
      return new ProxyInfo(proto, 
                           actorName,
                           actorSysName,
                           Localhost,
                           LocalMode,
                           actorPath);
    }
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
