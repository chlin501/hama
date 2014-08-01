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
package org.apache.hama.bsp.v2

import akka.actor.Actor
import akka.actor.ActorRef
import akka.actor.ActorSystem
import akka.event.Logging
import org.apache.hadoop.io.IntWritable
import org.apache.hama.HamaConfiguration
import org.apache.hama.logging.Logger
import org.apache.hama.TestEnv
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

class Begin extends FirstSuperstep with Logger {

  override def compute(bsp: BSPPeer) {
    LOG.info("Begin superstep ...")
    val count = find[IntWritable]("count")
    count match {
      case null => {
        LOG.info("No count var found, initialize to 0 ...")
        collect[IntWritable]("count", new IntWritable(0))
      }
      case i: IntWritable => 
        throw new RuntimeException("Initial value should be null, but "+i.get+
                                   "found!")
    }
  }

  override def next(): Class[_ <: Superstep] = classOf[Second]

}

class Second extends Superstep with Logger {

  override def compute(bsp: BSPPeer) {
    val count = find[IntWritable]("count")
    count match {
      case i: IntWritable => {
        if(0 != i.get) 
          throw new RuntimeException("Expect 0, but "+i.get+" found!")
        collect("count", new IntWritable(i.get+1))
        LOG.info("Count value is "+(i.get+1))
      }
      case v@_ => throw new RuntimeException("Expect 0 but "+v+" found!")
    }
  }

  override def next(): Class[_ <: Superstep] = classOf[End]
}

class End extends FinalSuperstep with Logger {

  override def compute(bsp: BSPPeer) {
    val count = find[IntWritable]("count")
    count match {
      case i: IntWritable => {
        if(1 != i.get) 
          throw new RuntimeException("Expect 1, but "+i.get+" found!")
        collect("count", new IntWritable(i.get+1))
        LOG.info("Count value is "+(i.get+1))
      }
      case v@_ => throw new RuntimeException("Expect 1 but "+v+" found!")
    } 
  }

}

class MockSuperstepBSP(testConf: HamaConfiguration) extends SuperstepBSP {

  override def commonConf(peer: BSPPeer): HamaConfiguration = testConf

  def getSupersteps(): Map[String, Superstep] = supersteps

}

@RunWith(classOf[JUnitRunner])
class TestSuperstepBSP extends TestEnv(ActorSystem("TestSuperstepBSP")) {

  override def beforeAll {
    super.beforeAll
    val classes = "%s,%s,%s".format(classOf[Begin].getName, 
                                    classOf[Second].getName,
                                    classOf[End].getName)
    testConfiguration.set("hama.supersteps.class", classes)
  }

  it("test superstep bsp function.") {
    val superstepBSP = new MockSuperstepBSP(testConfiguration)
    val dummy = new DummyBSPPeer
    superstepBSP.setup(dummy)
    superstepBSP.bsp(dummy)
    superstepBSP.getSupersteps.find( entry => 
      entry._2.isInstanceOf[FinalSuperstep]
    ) match {
      case Some(found) => {
        val key = found._1
        val value = found._2.asInstanceOf[Superstep]
        LOG.info("Found Superstep key "+key+" value "+value)
        assert(classOf[End].getName.equals(key))
        value.getVariables.get("count") match {
          case None => 
            throw new RuntimeException("Variable `count' not found in map!")
          case Some(v) => assert(2 == v.asInstanceOf[IntWritable].get)
        }
      }
      case _ => throw new RuntimeException("Can't find FinalSuperstep!")
    }
  }
}
