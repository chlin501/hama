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

import akka.actor.ActorRef
import org.apache.hama.bsp.BSPJobID
import org.apache.hama.bsp.v2.Task
import org.apache.hama.HamaConfiguration
import org.apache.hama.LocalService
import org.apache.hama.RemoteService
import org.apache.hama.util.ActorLocator
import org.apache.hama.util.JobTasksTrackerLocator

/**
 * Report tasks status to JobTasksReporter.
 */
final class TasksReporter(conf: HamaConfiguration) extends LocalService 
                                                   with RemoteService 
                                                   with ActorLocator {

  var tracker: ActorRef = _

/*
  val jobTasksTrackerInfo =
    new ProxyInfo.Builder().withConfiguration(conf).
                            withActorName("jobTasksTracker").
                            appendRootPath("bspmaster").
                            appendChildPath("monitor").
                            appendChildPath("jobTasksTracker").
                            buildProxyAtMaster
  val jobTasksTrackerPath = jobTasksTrackerInfo.getPath
*/

  override def configuration: HamaConfiguration = conf

  override def name: String = "tasksReporter"

  override def initializeServices {
    //lookup("jobTasksTracker", jobTasksTrackerPath)
    lookup("jobTasksTracker", locate(JobTasksTrackerLocator(configuration)))
  }

  override def afterLinked(proxy: ActorRef) = tracker = proxy

  override def receive = {
    isServiceReady orElse
    ({case newTask: Task => tracker ! newTask
    }: Receive) orElse isProxyReady orElse timeout orElse unknown
  } 
}
