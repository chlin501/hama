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
package org.apache.hama.groom.monitor

import akka.actor._
import java.lang.management.GarbageCollectorMXBean
import java.lang.management.ManagementFactory
import java.lang.management.MemoryMXBean
import java.lang.management.MemoryUsage
import java.lang.management.ThreadInfo
import java.lang.management.ThreadMXBean
import org.apache.hama._
import org.apache.hama.groom._
import org.apache.hama.groom.monitor._
import org.apache.hama.groom.monitor.Metrics._
import org.apache.hama.monitor._


/**
 * Report sys metrics information.
 */
final class SysMetricsReporter(conf: HamaConfiguration) extends LocalService 
                                                        with RemoteService {
  private var tracker: ActorRef = _

  private val memoryMXBean: MemoryMXBean = 
    ManagementFactory.getMemoryMXBean
  private var gcBeans: java.util.List[GarbageCollectorMXBean] =
    ManagementFactory.getGarbageCollectorMXBeans
  private val threadMXBean: ThreadMXBean = 
    ManagementFactory.getThreadMXBean
  private val M: Long = 1024*1024;

  override def configuration: HamaConfiguration = conf

  override def name: String = "sysMetricsReporter"

  override def initializeServices {
   // lookup("sysMetricsTracker", sysMetricsTracker)
  }

  override def afterLinked(proxy: ActorRef) = {
    tracker = proxy
  }

  override def receive = isServiceReady orElse unknown

  private def memory(record: MetricsRecord ) {
    val memNonHeap: MemoryUsage = memoryMXBean.getNonHeapMemoryUsage
    val memHeap: MemoryUsage = memoryMXBean.getHeapMemoryUsage
    record.add(new Metric(MemNonHeapUsedM, memNonHeap.getUsed / M))
    record.add(new Metric(MemNonHeapCommittedM,
                           memNonHeap.getCommitted / M))
    record.add(new Metric(MemHeapUsedM, memHeap.getUsed / M))
    record.add(new Metric(MemHeapCommittedM, memHeap.getCommitted / M))

    LOG.debug("{}: {}", MemNonHeapUsedM.description, memNonHeap.getUsed / M)
    LOG.debug("{}: {}", MemNonHeapCommittedM.description, 
              memNonHeap.getCommitted / M)
    LOG.debug("{}: {}", MemHeapUsedM.description, memHeap.getUsed / M)
    LOG.debug("{}: {}", MemHeapCommittedM.description, 
              memHeap.getCommitted / M)
  }

  private def gc(record: MetricsRecord) {
    var count: Long = 0
    var timeMillis: Long = 0
    val itr = gcBeans.iterator
    while(itr.hasNext) {
      val gcBean = itr.next
      val c = gcBean.getCollectionCount
      val t = gcBean.getCollectionTime
      val name = gcBean.getName
      record.add(new Metric("GcCount"+name, c))
      record.add(new Metric("GcTimeMillis"+name, t))
      count += c;
      timeMillis += t; 
    }
    record.add(new Metric(GcCount, count));
    record.add(new Metric(GcTimeMillis, timeMillis));

    LOG.debug("{}: {}", GcCount.description, count);
    LOG.debug("{}: {}", GcTimeMillis.description, timeMillis);
  }
}
