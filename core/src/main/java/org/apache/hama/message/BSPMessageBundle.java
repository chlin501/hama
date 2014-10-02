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
package org.apache.hama.message;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hama.message.compress.BSPMessageCompressor;

/**
 * BSPMessageBundle stores a group of messages so that they can be sent in batch
 * rather than individually.
 * 
 */
public class BSPMessageBundle<M extends Writable> implements Writable,
    Iterable<M> {

  public static final Log LOG = LogFactory.getLog(BSPMessageBundle.class);

  private BSPMessageCompressor compressor = null;
  private long threshold = 128;

  private String className = null;
  private int bundleSize = 0;
  private int bundleLength = 0;

  ByteArrayOutputStream byteBuffer = null;
  DataOutputStream bufferDos = null;

  ByteArrayInputStream bis = null;
  DataInputStream dis = null;

  /**
   * Record messages' hash value before they are compressed. So it can be 
   * checked if bundles are equal. If frequency of hash collision is too high,
   * we may employ other way to check equality between bundles.
   */
  protected List<Integer> hashes = new ArrayList<Integer>();

  public BSPMessageBundle() {
    byteBuffer = new ByteArrayOutputStream();
    bufferDos = new DataOutputStream(byteBuffer);

    bundleSize = 0;
    bundleLength = 0;
  }

  ByteArrayOutputStream mbos = null;
  DataOutputStream mdos = null;
  ByteArrayInputStream mbis = null;
  DataInputStream mdis = null;

  public byte[] serialize(M message) throws IOException {
    mbos = new ByteArrayOutputStream();
    mdos = new DataOutputStream(mbos);
    message.write(mdos);
    return mbos.toByteArray();
  }

  private byte[] compressed;
  private byte[] serialized;

  public void addMessage(M ... messages) {
    for(M msg: messages) addMessage(msg);
  }

  /**
   * Add message to this bundle.
   * 
   * @param message BSPMessage to add.
   */
  public void addMessage(M message) {
    try {
      hashes.add(message.hashCode());
      serialized = serialize(message);

      if (compressor != null && serialized.length > threshold) {
        bufferDos.writeBoolean(true);
        compressed = compressor.compress(serialized);
        bufferDos.writeInt(compressed.length);
        bufferDos.write(compressed);

        bundleLength += compressed.length;
      } else {
        bufferDos.writeBoolean(false);
        bufferDos.write(serialized);

        bundleLength += serialized.length;
      }
    } catch (IOException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    if (className == null) {
      className = message.getClass().getName();
    }
    bundleSize++;
  }

  public Iterator<M> iterator() {
    bis = new ByteArrayInputStream(byteBuffer.toByteArray());
    dis = new DataInputStream(bis);

    Iterator<M> it = new Iterator<M>() {
      M msg;
      byte[] decompressed;

      @Override
      public boolean hasNext() {
        try {
          if (dis.available() > 0) {
            return true;
          } else {
            return false;
          }
        } catch (IOException e) {
          return false;
        }
      }

      @SuppressWarnings("unchecked")
      @Override
      public M next() {
        boolean isCompressed = false;
        try {
          isCompressed = dis.readBoolean();
        } catch (IOException e1) {
          e1.printStackTrace();
        }

        Class<M> clazz = null;
        try {
          clazz = (Class<M>) Class.forName(className);
        } catch (ClassNotFoundException e) {
          LOG.error("Class was not found.", e);
        }
        msg = ReflectionUtils.newInstance(clazz, null);

        try {
          if (isCompressed) {
            int length = dis.readInt();
            compressed = new byte[length];
            dis.readFully(compressed);
            decompressed = compressor.decompress(compressed);

            mbis = new ByteArrayInputStream(decompressed);
            mdis = new DataInputStream(mbis);
            msg.readFields(mdis);
          } else {
            msg.readFields(dis);
          }

        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }

        return msg;
      }

      @Override
      public void remove() {
        // TODO Auto-generated method stub
      }
    };
    return it;
  }

  public int size() {
    return bundleSize;
  }

  public void setCompressor(BSPMessageCompressor compressor, long threshold) {
    this.compressor = compressor;
    this.threshold = threshold;
  }

  /**
   * @return the byte length of messages
   * @throws IOException
   */
  public long getLength() throws IOException {
    return bundleLength;
  }

  public List<Integer> hashesOfMessages() {
    return Collections.unmodifiableList(hashes);
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) return true;
    if (null == o) return false;
    if (getClass() != o.getClass()) return false; 

    final BSPMessageBundle<M> s = (BSPMessageBundle<M>) o;
    if(hashes.size() != s.hashes.size()) return false;
    if(0 != hashes.size() && 0 != s.hashes.size()) {
      for(int idx = 0; idx < hashes.size(); idx++) {
        final Integer hash = hashes.get(idx);
        final Integer sHash = s.hashes.get(idx);
        LOG.debug("Hash value: "+hash+" Hash value to be compared: "+sHash);
        if(null == hash || null == sHash) 
          throw new NullPointerException("Message's hash value not equal! "+
                                         "Hash: "+hash+" compared: "+sHash);
        if (!sHash.equals(hash)) return false;
      }
    }
    return true;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(bundleSize);
    if (bundleSize > 0) {
      for(Integer hash: hashes) {
        out.writeInt(hash.intValue());
      }
      out.writeUTF(className);
      byte[] messages = byteBuffer.toByteArray();
      out.writeInt(messages.length);
      out.write(messages);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int numMessages = in.readInt();
    if (numMessages > 0) {
      for(int idx = 0; idx < numMessages; idx++) {
        final Integer hash = in.readInt();
        hashes.add(hash);
      }
      className = in.readUTF();
      int bytesLength = in.readInt();
      byte[] temp = new byte[bytesLength];
      in.readFully(temp);
      bufferDos.write(temp);
    }
  }

}
