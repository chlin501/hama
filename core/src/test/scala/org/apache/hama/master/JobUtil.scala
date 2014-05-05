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

import java.util.Random
import java.io.File
import java.io.FileOutputStream
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hama.bsp.BSPJobID
import org.apache.hama.bsp.v2.IDCreator
import org.apache.hama.bsp.v2.IDCreator._
import org.apache.hama.fs.HDFS
import org.apache.hama.HamaConfiguration
import org.apache.hama.util.Logger

class MockHDFS extends HDFS {

  def getHDFS: FileSystem = hdfs
}

trait JobUtil extends Logger {
  
  val random = new Random()

  val op: MockHDFS = new MockHDFS()

  def qualified(path: Path): Path = path.makeQualified(op.getHDFS)  

  def fsQualified(path: Path): Path = op.getHDFS.makeQualified(path)

  //* Below are Job related functions. *

  def createJobId(identifier: String, id: Int): BSPJobID =
    IDCreator.newBSPJobID.withId(identifier).withId(id).build

  @throws(classOf[Exception])
  def createJobFile(conf: HamaConfiguration): String = {
    val subDir = getSubmitJobDir(conf)
    createIfAbsent(subDir)
    val jobFilePath = new Path(subDir, "job.xml")
    LOG.info("jobFilePath (job.xml): %s".format(jobFilePath))
    var path = qualified(jobFilePath).toString
    path = path.replaceAll("file:", "")
    LOG.info("Path string of jobFilePath: %s".format(path))
    var out: FileOutputStream = null
    try {
      val file = new File(path)
      out = new FileOutputStream(file, false)
      conf.writeXml(out);
    } finally {
      if(null != out) {
        LOG.info("Close OutputStream: %s".format(out))
        out.close();
      }
    }
    LOG.info("JobFile is created at %s".format(path))
    path
  }

  def getSystemDir(conf: HamaConfiguration): Path = {
    val sysDir = new Path(conf.get("bsp.system.dir", "/tmp/hadoop/bsp/system"))
    createIfAbsent(sysDir)
    fsQualified(sysDir)
  }

  def createIfAbsent(path: Path) = if(!op.exists(path)) {
    LOG.info("Path %s is absent so creating ...".format(path))
    op.mkdirs(path)
  }

  def getSubmitJobDir(conf: HamaConfiguration): Path = {
    new Path(getSystemDir(conf),
             "submit_" + Integer.toString(Math.abs(random.nextInt), 36))
  }
}
