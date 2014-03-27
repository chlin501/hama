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
package org.apache.hama.groom.monitor;

public enum Metrics {
  MemNonHeapUsedM("Non-heap memory used in MB"),
  MemNonHeapCommittedM("Non-heap memory committed in MB"),
  MemHeapUsedM("Heap memory used in MB"),
  MemHeapCommittedM("Heap memory committed in MB"),
  GcCount("Total GC count"),
  GcTimeMillis("Total GC time in milliseconds"),
  ThreadsNew("Number of new threads"),
  ThreadsRunnable("Number of runnable threads"),
  ThreadsBlocked("Number of blocked threads"),
  ThreadsWaiting("Number of waiting threads"),
  ThreadsTimedWaiting("Number of timed waiting threads"),
  ThreadsTerminated("Number of terminated threads");

  private final String desc;

  Metrics(String desc) { this.desc = desc; }
  public String description() { return desc; }

  @Override public String toString(){
    return "name "+name()+" description "+desc;
  }
}
