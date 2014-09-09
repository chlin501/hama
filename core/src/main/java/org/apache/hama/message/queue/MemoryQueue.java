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
package org.apache.hama.message.queue;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.HamaConfiguration;

/**
 * LinkedList backed queue structure for bookkeeping messages.
 */
public final class MemoryQueue<M extends Writable> 
      implements SynchronizedQueue<M>, Viewable<M> {

  private final ConcurrentLinkedQueue<M> deque = new ConcurrentLinkedQueue<M>();
  private HamaConfiguration conf;

  @Override
  public final void addAll(Iterable<M> col) {
    for (M m : col)
      deque.add(m);
  }

  @Override
  public void addAll(MessageQueue<M> otherqueue) {
    M poll = null;
    while ((poll = otherqueue.poll()) != null) {
      deque.add(poll);
    }
  }

  @Override
  public final void add(M item) {
    deque.add(item);
  }

  @Override
  public final void clear() {
    deque.clear();
  }

  @Override
  public final M poll() {
    return deque.poll();
  }

  @Override
  public final int size() {
    return deque.size();
  }

  @Override
  public final Iterator<M> iterator() {
    return deque.iterator();
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = (HamaConfiguration)conf;
  }

  @Override
  public Configuration getConf() {
    return (Configuration) this.conf;
  }

  // not doing much here
  @Override
  public void init(HamaConfiguration conf, TaskAttemptID id) { }

  @Override
  public void close() {
    this.clear();
  }

  @Override
  public void prepareRead() { }

  @Override
  public void prepareWrite() { }

  @Override
  public boolean isMessageSerialized() {
    return false;
  }

  @Override
  public boolean isMemoryBasedQueue() {
    return true;
  }

  @Override
  public MessageQueue<M> getMessageQueue() {
    return this;
  }

  @Override 
  public List<M> view() {
    return Collections.unmodifiableList(Arrays.asList((M[])deque.toArray(new Object[deque.size()])));
  }
}
