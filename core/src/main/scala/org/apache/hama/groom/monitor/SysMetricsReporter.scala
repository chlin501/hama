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
import java.lang.Thread.State._
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hama._
import org.apache.hama.groom._
import org.apache.hama.master.monitor._
import org.apache.hama.monitor.metrics._
import org.apache.hama.monitor.metrics.Metrics._
import scala.concurrent.duration._
import scala.util.control._


/**
 * Report sys metrics information.
 */
final class SysMetricsReporter(conf: HamaConfiguration) extends LocalService 
                                                        with RemoteService {
  private var tracker: ActorRef = _
  private var cancellable: Cancellable = _

  val sysMetricsTrackerInfo =
    ProxyInfo("sysMetricsTracker",
              conf.get("bsp.master.actor-system.name", "MasterSystem"),
              conf.get("bsp.master.address", "127.0.0.1"),
              conf.getInt("bsp.master.port", 40000),
              "bspmaster/monitor/sysMetricsTracker")

  val sysMetricsTrackerPath = sysMetricsTrackerInfo.path

  private val host = conf.get("bsp.groom.hostname", "0.0.0.0")

  /* We don't use rpc, so remove bsp.gorom.rpc.port */
  private val port = conf.getInt("bsp.groom.port", 50000)

  private val groomServerName = "groom_"+host+"_"+port

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
    lookup("sysMetricsTracker", sysMetricsTrackerPath)
  }

  override def afterLinked(proxy: ActorRef) = {
    tracker = proxy
    LOG.debug("Sending metrics stat to {}", tracker)
    import context.dispatcher // cancel when actor stopped
    cancellable = 
      context.system.scheduler.schedule(0.seconds, 5.seconds, tracker, 
                                        sampling)
  }

  def sampling(): MetricsRecord = {
    val record: MetricsRecord = 
      new MetricsRecord(groomServerName, "jvm", "Jvm metrics stats.")
    memory(record)
    gc(record)
    threads(record)
    record
  }

  override def receive = isServiceReady orElse isProxyReady orElse timeout orElse unknown

  private def memory(record: MetricsRecord ) {
    val memNonHeap: MemoryUsage = memoryMXBean.getNonHeapMemoryUsage
    val memHeap: MemoryUsage = memoryMXBean.getHeapMemoryUsage

    val nonHeapUsed = new LongWritable(memNonHeap.getUsed / M)
    record.add(new Metric(MemNonHeapUsedM, nonHeapUsed))

    val nonHeapCommitted = new LongWritable(memNonHeap.getCommitted / M)
    record.add(new Metric(MemNonHeapCommittedM, nonHeapCommitted))

    val heapUsed = new LongWritable(memHeap.getUsed / M)
    record.add(new Metric(MemHeapUsedM, heapUsed))

    val heapCommitted = new LongWritable(memHeap.getCommitted / M)
    record.add(new Metric(MemHeapCommittedM, heapCommitted))

    LOG.debug("{}: {}", MemNonHeapUsedM.description, nonHeapUsed)
    LOG.debug("{}: {}", MemNonHeapCommittedM.description, nonHeapCommitted)
    LOG.debug("{}: {}", MemHeapUsedM.description, heapUsed)
    LOG.debug("{}: {}", MemHeapCommittedM.description, heapCommitted)
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
      record.add(new Metric("GcCount"+name, "GcCount"+name+" metric.", 
                            classOf[LongWritable], new LongWritable(c)))
      record.add(new Metric("GcTimeMillis"+name, 
                            "GcTimeMillis"+name+" metric.", 
                            classOf[LongWritable], new LongWritable(t)))
      count += c
      timeMillis += t
    }
    record.add(new Metric(GcCount, new LongWritable(count)))
    record.add(new Metric(GcTimeMillis, new LongWritable(timeMillis)))

    LOG.debug("{}: {}", GcCount.description, new LongWritable(count))
    LOG.debug("{}: {}", GcTimeMillis.description, 
                        new LongWritable(timeMillis))
  }

  private def threads(record: MetricsRecord){
    var threadsNew = 0
    var threadsRunnable = 0
    var threadsBlocked = 0
    var threadsWaiting = 0
    var threadsTimedWaiting = 0
    var threadsTerminated = 0
    val threadIds: Array[Long] = threadMXBean.getAllThreadIds

    val loop = new Breaks;
    loop.breakable {
      threadMXBean.getThreadInfo(threadIds, 0).foreach ( threadInfo => {
        if (threadInfo != null) {
          val state = threadInfo.getThreadState
          if(NEW.equals(state)){
           threadsNew += 1
            loop.break
          }else if(RUNNABLE.equals(state)){
            threadsRunnable += 1
            loop.break
          }else if(BLOCKED.equals(state)){
            threadsBlocked += 1
            loop.break
          }else if(WAITING.equals(state)){
            threadsWaiting += 1
            loop.break
          }else if(TIMED_WAITING.equals(state)){
            threadsTimedWaiting += 1
            loop.break
          }else if(TERMINATED.equals(state)){
            threadsTerminated += 1
            loop.break
          }
        }
      })
    }

    val tnew = new IntWritable(threadsNew)
    record.add(new Metric(ThreadsNew, tnew))

    val trunnable = new IntWritable(threadsRunnable)
    record.add(new Metric(ThreadsRunnable, trunnable))

    val tblocked = new IntWritable(threadsBlocked)
    record.add(new Metric(ThreadsBlocked, tblocked))

    val twaiting = new IntWritable(threadsWaiting)
    record.add(new Metric(ThreadsWaiting, twaiting))

    val ttimedwaiting = new IntWritable(threadsTimedWaiting)
    record.add(new Metric(ThreadsTimedWaiting, ttimedwaiting))

    val tterminated = new IntWritable(threadsTerminated)
    record.add(new Metric(ThreadsTerminated, tterminated))

    LOG.debug("{}: {}", ThreadsNew.description, tnew)
    LOG.debug("{}: {}", ThreadsRunnable.description, trunnable)
    LOG.debug("{}: {}", ThreadsBlocked.description, tblocked)
    LOG.debug("{}: {}", ThreadsWaiting.description, twaiting)
    LOG.debug("{}: {}", ThreadsTimedWaiting.description, ttimedwaiting)
    LOG.debug("{}: {}", ThreadsTerminated.description, tterminated)
  }

}
