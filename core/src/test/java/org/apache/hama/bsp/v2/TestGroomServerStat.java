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

import junit.framework.TestCase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hama.bsp.BSPJobID;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.hama.bsp.TaskID;

public class TestGroomServerStat extends TestCase {

  final Log LOG = LogFactory.getLog(TestGroomServerStat.class);
  final int maxTasks = 3;

  public void testGroomServerStat() throws Exception {
    final GroomServerStat stat = new GroomServerStat("groom_localhost_60001", 
                                                     "localhost", 
                                                     60001, 
                                                     maxTasks);
    assertNotNull("GroomServerStat is not null.", stat);
    // queue
    stat.addToQueue("job1_task1");
    stat.addToQueue("job2_task2");
    // slots
    stat.mark(0, "job3_task4");
    stat.mark(1, GroomServerStat.toNullString());
    stat.mark(2, "job4_task1");
    final String[] queue = stat.getQueue();
    assertEquals("Queue size should equal.", 2, queue.length);
    assertEquals("The first element is job1_task1", "job1_task1", queue[0]);
    assertEquals("The second element is job2_task2", "job2_task2", queue[1]);

    final String[] slots = stat.getSlots();
    assertEquals("Slots size should equal.", maxTasks, slots.length);
    assertEquals("The first slot is job3_task4", "job3_task4", slots[0]);
    assertEquals("The second slot is (null)", "(null)", slots[1]);
    assertEquals("The third slot is job4_task1", "job4_task1", slots[2]);
  }


}
