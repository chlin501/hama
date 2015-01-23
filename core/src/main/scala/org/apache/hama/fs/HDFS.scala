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

import java.io.Closeable
import java.io.FileNotFoundException
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.util.Arrays
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.io.IOUtils

import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hama.HamaConfiguration

class HDFS extends Operation {

  private var conf = new HamaConfiguration() 
  protected var hdfs: FileSystem = _

  override protected[fs] def setFs(fs: FileSystem) = this.hdfs = fs

  override def configuration: HamaConfiguration = this.conf

  override def initialize(conf: HamaConfiguration) {
    this.conf = conf
    require(null != configuration, "HamaConfiguration is missing for HDFS!")
    this.hdfs = FileSystem.get(configuration)
  } 
 
  @throws(classOf[IOException])
  protected def validate() {
    if(null == hdfs)
      throw new IOException("Unable to instantiate hadoop FileSystem.")
  }

  @throws(classOf[IOException])
  override def mkdirs(path: Path): Boolean = {
    validate
    hdfs.mkdirs(path)
  }

  @throws(classOf[IOException])
  override def mkdirs(path: Path, permission: FsPermission): Boolean = {
    validate
    hdfs.mkdirs(path, permission)
  }

  @throws(classOf[IOException])
  override def create(path: Path, overwrite: Boolean = true): OutputStream = {
    validate
    hdfs.create(path, overwrite) 
  }

  @throws(classOf[IOException])
  override def create(path: Path, replication: Short, blockSize: Long, 
                      overwrite: Boolean): OutputStream = {
    validate
    val bufferSize = configuration.getInt("io.file.buffer.size", 4096)
    hdfs.create(path, overwrite, bufferSize, replication, blockSize)
  }

  @throws(classOf[IOException])
  override def append(path: Path): OutputStream = {
    validate
    hdfs.append(path)
  }

  @throws(classOf[IOException])
  override def open(path: Path): InputStream = {
    validate
    hdfs.open(path) 
  }

  @throws(classOf[IOException])
  override def exists(path: Path): Boolean = {
    validate
    hdfs.exists(path) 
  }

  @throws(classOf[IOException])
  override def remove(path: Path): Boolean = hdfs.delete(path, true)

  @throws(classOf[IOException])
  override def move(from: Path, to: Path) = hdfs.rename(from, to) 

  @throws(classOf[IOException])
  @throws(classOf[FileNotFoundException])
  override def list[FileStatus](path: Path): java.util.List[FileStatus] = 
    Arrays.asList(hdfs.listStatus(path).asInstanceOf[Array[FileStatus]]:_*)

  override def copyToLocal(from: Path)(to: Path) {
    hdfs.copyToLocalFile(from, to)
  }

  override def copyFromLocal(from: Path)(to: Path) {
    hdfs.copyFromLocalFile(from, to)
  }

  /**
   * Default system directory is set to "/tmp/hadoop/bsp/system".
   */
  override def getSystemDirectory: Path = { 
    val sysDir = configuration.get("bsp.system.dir", "/tmp/hadoop/bsp/system")
    hdfs.makeQualified(new Path(sysDir))
  }

  override def getWorkingDirectory: Path = hdfs.getWorkingDirectory
  
  override def local: Operation = {
    val confx = new HamaConfiguration()
    confx.setClass("bsp.fs.class", classOf[HDFSLocal], classOf[Operation]) 
    Operation.get(confx)
  }

  override def makeQualified(path: Path): String = 
    path.makeQualified(hdfs).toString

  override def setWorkingDirectory(path: Path) = hdfs.setWorkingDirectory(path)

  override def close(out: Closeable) = IOUtils.closeStream(out) 
  
}
