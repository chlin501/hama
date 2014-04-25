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

import java.util.Random
import java.io.File
import java.io.FileOutputStream
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hama.HamaConfiguration

/**
 * Wraper of {@link org.apache.hadoop.fs.LocalFileSystem}.
trait TestFileSystem {
  
  protected val fs = new MockFileSystem(testConfiguration)
  
  protected val random = new Random()

  protected def testConfiguration: HamaConfiguration 

  protected def mockFs: FileSystem = fs

  protected def log(msg: String) = { println(msg) }

  //* Below are Job related functions. *

  @throws(classOf[Exception])
  protected def createJobFile: String = {
    log("FileSystem: %s".format(mockFs))
    val subDir = submitJobDir
    createIfAbsent(subDir)
    val jobFilePath = new Path(subDir, "job.xml")
    log("jobFilePath: %s".format(jobFilePath))
    var path = jobFilePath.makeQualified(mockFs).toString
    path = path.replaceAll("file:", "")
    log("Path string of jobFilePath: %s".format(path))
    var out: FileOutputStream = null
    try {
      val file = new File(path)
      out = new FileOutputStream(file, false)
      testConfiguration.writeXml(out);
    } finally {
      log("OutputStream is null? %s".format(out))
      out.close();
    }
    path
  }

  protected def getSystemDir: Path = {
    val sysDir = new Path(testConfiguration.get("bsp.system.dir",
                                            "/tmp/hadoop/bsp/system"))
    createIfAbsent(sysDir)
    mockFs.makeQualified(sysDir)
  }

  protected def createIfAbsent(path: Path) = if(!mockFs.exists(path)) {
    log("Create path %%ss".format(path))
    mockFs.mkdirs(path)
  }

  protected def submitJobDir: Path = {
    new Path(getSystemDir,
             "submit_" + Integer.toString(Math.abs(random.nextInt), 36))
  }
}
*/
