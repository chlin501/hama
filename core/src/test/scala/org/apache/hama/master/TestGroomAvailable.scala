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
package org.apache.hama.master

import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.matchers._
import org.scalatest.FunSpec
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner]) 
class TestGroomAvailable extends FunSpec with ShouldMatchers {

  val LOG = LogFactory.getLog(classOf[TestGroomAvailable])

  it("groom available init function") {
    val avail1 = GroomAvailable("groom_test_40000", 3, Array.empty[Int])
    LOG.info("avail1 maxTasks: "+avail1.maxTasks+" free slots: "+
             avail1.freeSlots.size)
    assert(avail1.maxTasks > avail1.freeSlots.size)
  }

  it("throw a runtime exception if freeSlots size > maxTasks") {
    intercept[RuntimeException]{
      GroomAvailable("groom_test_40000", 3, Array(1, 4, 2, 5))
    }
  } 
}
