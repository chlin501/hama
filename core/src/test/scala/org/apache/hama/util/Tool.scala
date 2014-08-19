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
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.IOException
import java.util.jar.Attributes
import java.util.jar.JarEntry
import java.util.jar.JarOutputStream
import java.util.jar.Manifest
import scala.tools.nsc.{Global, Settings}
import scala.collection.immutable.Stream

/**
 * TODO: apply this for testing add jar to classpath.
 */
object Tool {

  /*
  def main(args: Array[String]) {
    compile("/tmp/compile/src", "/tmp/compile/classes")
    jar("/tmp/compile/classes", "/tmp/test.jar")
  }
  */

  /**
   * Compile the source code located at src directory to target directory.
   * @param src is the source directory.
   * @param target is the classes directory.
   */
  @throws(classOf[IOException])
  def compile(src: String, target: String) {
    target match {
      case null|"" => throw new IOException("Target dir string is missing!")
      case dir@_ => {
        val targetDir = new File(dir)
        if(!targetDir.exists) targetDir.mkdirs
        val settings = new Settings
        settings.outputDirs.setSingleOutput(targetDir.toString)
        val global = new Global(settings)
        val compiler = new global.Run
        src match {
          case null|"" => throw new IOException("Source dir string is missing!")
          case srcDir@_ =>
            compiler.compile(new File(srcDir).listFiles.toList.map { file => {
              file.getPath
            }})
        }
      }
    }
  }
 
  /**
   * Jar files located within a single directory.
   * @param targetDir contains all class files to be zipped.
   * @param jarPath denotes the jar file dest path to be created.
   * @param mainClass would be noted if exists; default to empty string.
   */
  @throws(classOf[IOException])
  def jar(targetDir: String, jarPath: String, mainClass: String = "") {
    val manifest = new Manifest()
    val mainAttributes = manifest.getMainAttributes
    mainAttributes.put(Attributes.Name.MANIFEST_VERSION, "None")
    mainClass match {
      case null|"" =>
      case _ => mainAttributes.put(Attributes.Name.MAIN_CLASS, mainClass)
    }
    val out = new JarOutputStream(new FileOutputStream(jarPath), manifest)
    val dir = new File(targetDir)
    if(!dir.exists) throw new IOException("Path "+targetDir+" does not exist!")
    dir.listFiles.foreach(file => {
      val entry = new JarEntry(file.getName)
      entry.setTime(file.lastModified)
      out.putNextEntry(entry)
      var buf = new Array[Byte](1024)
      val in = new FileInputStream(file)
      Stream.continually(in.read(buf)).
             takeWhile(_ != -1).
             foreach(out.write(buf, 0, _))
      in.close
      out.closeEntry
    })
    out.close
  }
}
