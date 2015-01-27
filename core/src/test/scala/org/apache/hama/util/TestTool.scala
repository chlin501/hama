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
package org.apache.hama.util

import java.io.File

import org.apache.hama.TestEnv
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TestTool extends TestEnv("TestTool") {

  //override def afterAll { }

  def srcRoot(): String = new File(Tool.pwd+"/src/test/resources/sample").
    getAbsolutePath

  def target(): String = {
    val d = new File(testRootPath, "target").getAbsolutePath
    mkdirs(d)
    d
  }

  def output(): String = new File(testRootPath, "sample.jar").getAbsolutePath 

  def sources(): List[String] = 
    List[String](new File(srcRoot, "Sample.scala").getAbsolutePath)

  it("test tool functions.") {
    Tool.compile(target, sources)
    Tool.jar(target, output)
    assert(true == new File(output).exists)
  }
}
