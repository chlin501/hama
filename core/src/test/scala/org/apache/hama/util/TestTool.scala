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

import java.io.BufferedInputStream
import java.io.BufferedOutputStream
import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.util.jar.JarEntry
import java.util.jar.JarOutputStream
import org.apache.hama.TestEnv
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TestTool extends TestEnv("TestTool") {

  def loop(src: File): Array[String] = 
    src.isDirectory match {
      case true => Array.concat(Array(src.getPath+"/"), 
        src.listFiles.map { nested => loop(nested) }.flatten)
      case false => Array(src.getPath)
    }

  it("test tool functions.") {
    val output = new JarOutputStream(new BufferedOutputStream(new FileOutputStream("/tmp/test.jar")))
    val root = "/tmp/test/classes"
    loop(new File(root)).filter( entry => 
      !entry.equals(root+"/")
    ).map { entry => {
      val file = new File(entry)
      file.isDirectory match {
        case true => {
          val newEntry = entry.replace(root+"/", "")
          val e = new JarEntry(newEntry)
          e.setTime(file.lastModified)
          output.putNextEntry(e)
          output.closeEntry
        }
        case false => {
          val newEntry = entry.replace(root+"/", "")
          val e = new JarEntry(newEntry)
          e.setTime(file.lastModified)
          output.putNextEntry(e)
          val in = new BufferedInputStream(new FileInputStream(entry))
          Stream.continually(in.read).takeWhile(-1!=).foreach(output.write)
          output.closeEntry
          in.close
        }
      }
    }}
    output.close

  }
}
