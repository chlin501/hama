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
import org.apache.hama.logging.CommonLog
import scala.tools.nsc.Global
import scala.tools.nsc.Settings 

object Tool extends CommonLog {

  val pwd = System.getProperty("user.dir")

  /**
   * Compile source code based on the output directory. 
   * @param target is the output directory where classes files are written.
   * @param sources are a list of source files.
   */
  def compile(target: String, sources: List[String]) {
    val s = new Settings() 
    val scalaHome = System.getProperty("scala.home")
    require(null != scalaHome, "Scala home is not defined!")
    val scalalibrary = new File(new File(scalaHome, "lib"), "scala-library.jar")
    require(scalalibrary.exists, "Can't find scala-library.jar!")
    val scalalibraryjar = scalalibrary.getAbsolutePath
    s.classpath.append(scalalibraryjar)
    s.bootclasspath.append(scalalibraryjar)
    s.outputDirs.setSingleOutput(new File(target).getAbsolutePath)
    val global = new Global(s)
    val runner = new global.Run
    runner.compile(sources)
  }

  protected[util] def loop(src: File): Array[String] = src.isDirectory match {
    case true => Array.concat(Array(src.getPath+"/"), 
      src.listFiles.map { nested => loop(nested) }.flatten)
    case false => Array(src.getPath)
  } 

  /**
   * Jar a list of files under a specific directory.
   * @param srcRoot is the directory under which all class files will be zipeed.
   * @param targetDir is the output target directory under which the jar file 
   *                  are written.
   */
  def jar(srcRoot: String, targetDir: String) {
    val rootDir = new File(srcRoot)
    val output = new JarOutputStream(new BufferedOutputStream(
      new FileOutputStream(targetDir)))
    loop(rootDir).filter( entry => !entry.equals(srcRoot+"/")).map { entry =>
    val file = new File(entry)
    file.isDirectory match {
      case true => {
        val newEntry = entry.replace(srcRoot+"/", "")
        val e = new JarEntry(newEntry)
        e.setTime(file.lastModified)
        output.putNextEntry(e)
        output.closeEntry
      }
      case false => {
        val newEntry = entry.replace(srcRoot+"/", "")
        val e = new JarEntry(newEntry)
        e.setTime(file.lastModified)
        output.putNextEntry(e)
        val in = new BufferedInputStream(new FileInputStream(entry))
        Stream.continually(in.read).takeWhile(-1!=).foreach(output.write)
        output.closeEntry
        in.close
      }
    }}
    output.close
  }

}
