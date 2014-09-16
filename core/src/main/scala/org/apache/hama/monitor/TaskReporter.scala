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
package org.apache.hama.monitor

import org.apache.hama.Agent
import org.apache.hama.Close
import org.apache.hama.HamaConfiguration
import org.apache.hama.util.Curator
import scala.util.Failure
import scala.util.Success
import scala.util.Try
import scala.collection.JavaConversions._

final case class Report(stat: TaskStat)

/**
 * This provides ways in writing task stat to external places such as zk.
 */
class TaskReporter extends Agent with Curator {

  /**
   * Close this task reporter.
   * @param Receive is a partial function.
   */
  protected def close: Receive = {
    case Close => context.stop(self)
  }

  protected def report: Receive = {
    case Report(stat) => doReport(stat)
  }

  protected def doReport(stat: TaskStat) {
    // TODO: transform stat to zk format 
    //       write to zk 
  }

  override def receive = report orElse close orElse unknown
}
