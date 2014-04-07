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
 
import com.typesafe.config.ConfigFactory

import akka.actor._
import akka.testkit._

import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import org.apache.hama._
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatest.WordSpecLike
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.MustMatchers
import scala.concurrent.duration._



object TestMasterSpec {
  val config = """
    akka {
      loglevel = "DEBUG"
    }
    """ 
}

@RunWith(classOf[JUnitRunner]) 
class TestMasterSpec extends TestKit(ActorSystem("TestMasterSpec", 
                                                 ConfigFactory.parseString(
                                                   TestMasterSpec.config))) 
                     with DefaultTimeout with ImplicitSender 
                     with WordSpecLike with MustMatchers with BeforeAndAfterAll {

  val LOG = LogFactory.getLog(classOf[TestMasterSpec])

  override def afterAll {
    shutdown(system)
  }

  "a master" must {
    "wait for other services" in {
      val master = TestActorRef(new Master(new HamaConfiguration))
      import system.dispatcher
      val cancellable = 
        system.scheduler.schedule(1.seconds, 1.seconds, master, Ready)
      var flag = false
      receiveWhile(10 seconds) {
        case msg => {
          LOG.info("Master returns "+msg)
          if(msg.equals(Ack("yes"))) {
            flag = true
            cancellable.cancel
          }
        }
      }
      assert(flag == true)
    }
  }
}
