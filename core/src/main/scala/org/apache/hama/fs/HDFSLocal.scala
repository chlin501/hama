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
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileStatus

import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hama.HamaConfiguration

class HDFSLocal extends Operation {

  private var conf = new HamaConfiguration
  private var localfs: FileSystem = _

  override protected[fs] def setFs(fs: FileSystem) = this.localfs = fs

  override def initialize(conf: HamaConfiguration) {
    this.conf = conf
    require(null != conf, "HamaConfiguration for HDFSLocal is missing!")
    this.localfs = FileSystem.getLocal(conf)
  }

  override def configuration: HamaConfiguration = this.conf
 
  @throws(classOf[IOException])
  protected def validate() {
    if(null == localfs)
      throw new IOException("Local FileSystem is not instantiated.")
  }

  @throws(classOf[IOException])
  override def mkdirs(path: Path): Boolean = {
    validate
    localfs.mkdirs(path)
  }

  @throws(classOf[IOException])
  override def mkdirs(path: Path, permission: FsPermission): Boolean = {
    validate
    localfs.mkdirs(path, permission)
  }

  @throws(classOf[IOException])
  override def setReplication(path: Path, replication: Short): Boolean = {
    validate
    localfs.setReplication(path, replication)
  }

  @throws(classOf[IOException])
  override def setPermission(path: Path, permission: FsPermission) {
    validate
    localfs.setPermission(path, permission)
  }

  @throws(classOf[IOException])
  override def create(path: Path, permission: FsPermission): OutputStream = {
    val out = create(path)
    localfs.setPermission(path, permission) 
    out 
  }

  @throws(classOf[IOException])
  override def create(path: Path, overwrite: Boolean = true): OutputStream = {
    validate
    localfs.create(path, overwrite)
  }

  @throws(classOf[IOException])
  override def create(path: Path, replication: Short, blockSize: Long,
                      overwrite: Boolean): OutputStream = {
    validate
    val bufferSize = configuration.getInt("io.file.buffer.size", 4096)
    localfs.create(path, overwrite, bufferSize, replication, blockSize)
  }

  @throws(classOf[IOException])
  override def append(path: Path): OutputStream = {
    validate
    localfs.append(path)
  }

  @throws(classOf[IOException])
  override def open(path: Path): InputStream = {
    validate
    localfs.open(path) 
  }

  @throws(classOf[IOException])
  override def exists(path: Path): Boolean = {
    validate
    localfs.exists(path) 
  }

  @throws(classOf[IOException])
  override def remove(path: Path): Boolean = localfs.delete(path, true)

  @throws(classOf[IOException])
  override def move(from: Path, to: Path) = localfs.rename(from, to) 

  @throws(classOf[IOException])
  @throws(classOf[FileNotFoundException])
  override def list[FileStatus](path: Path): java.util.List[FileStatus] = 
    Arrays.asList(localfs.listStatus(path).asInstanceOf[Array[FileStatus]]:_*)

  @throws(classOf[IOException])
  override def copyToLocal(from: Path)(to: Path) { 
    throw new UnsupportedOperationException("Operation copyToLocal not "+
                                            "supported.")
  }

  @throws(classOf[IOException])
  override def copyFromLocal(from: Path)(to: Path) {
    throw new UnsupportedOperationException("Operation copyFromLocal not "+
                                            "supported.")
  }

  /**
   * Default system directory is set to "/tmp/hadoop/bsp/system".
   */
  override def getSystemDirectory: Path = { 
    val sysDir = configuration.get("bsp.system.dir", "/tmp/hadoop/bsp/system")
    localfs.makeQualified(new Path(sysDir))
  }

  override def getWorkingDirectory: Path = {
    validate()
    localfs.getWorkingDirectory
  }
  
  override def local: Operation = this 

  override def makeQualified(path: Path): String = {
    validate()
    path.makeQualified(localfs).toString
  }

  override def setWorkingDirectory(path: Path) = {
    validate()
    localfs.setWorkingDirectory(path)
  }

  override def close(out: Closeable) = if(null != out) try {} finally { out.close }

}
