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
package org.apache.hama.fs

import akka.actor.ActorSystem
import akka.util.ByteString
import akka.util.ByteStringBuilder
import akka.util.ByteIterator
import org.apache.hadoop.fs.Path
import org.apache.hama.HamaConfiguration
import org.apache.hama.TestEnv
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TestHDFS extends TestEnv(ActorSystem("TestHDFS")) {

  var operation: Operation = _

  val testRootDir = testConfiguration.get("bsp.test.fs.root", "/tmp/hama/hdfs")

  def createIfAbsent(path: Path) {
    if(!operation.exists(path)) operation.mkdirs(path)
  }

  override def beforeAll {
    super.beforeAll
    testConfiguration.set("bsp.test.fs.root", "/tmp/hama/hdfs")
    operation = OperationFactory.get(testConfiguration)
    assert(null != operation)
    LOG.info("Create test root path at %s".format(testRootDir))
    createIfAbsent(new Path(testRootDir))
  }

  override def afterAll {
    operation.remove(new Path(testRootDir).getParent) 
    super.afterAll
  }

  it("test hdfs function.") {
    implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN
    LOG.info("Test hdfs file system operation")
    val dest = new Path(testRootDir, "parent")
    operation.mkdirs(dest)
    val target = new Path(dest, "data.txt")
    val out = operation.create(target)
    val builder = ByteString.newBuilder
    val result  = builder.putInt(213).result
    LOG.info("Array to be written: %s".format(result)) 
    val ref = result
    val expected = Array[Int](0, 0, 0, -43)
    var pos = 0  
    ref.foreach(byte => {
      LOG.info("Actual byte: %s expected: %s".format(byte, expected(pos)))
      assert(byte == expected(pos))
      pos+=1
    })
    out.write(result.toArray) 
    out.close
    val in = operation.open(target)
    val itr = result.iterator
    val content = itr.getInt
    LOG.info("Value stored in file is %s".format(content))
    assert(213 == content)
    in.close
  }
}
