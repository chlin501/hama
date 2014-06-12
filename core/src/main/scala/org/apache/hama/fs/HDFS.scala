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

import java.io.FileNotFoundException
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream
import java.util.Arrays
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.FileStatus

import org.apache.hadoop.fs.Path
import org.apache.hama.HamaConfiguration

object HDFS {

  def apply(conf: HamaConfiguration): HDFS = {
    val op = new HDFS()
    op.setConfiguration(conf) 
    op
  }

  def apply(fs: FileSystem): HDFS = {
    val op = new HDFS()
    op.setFileSystem(fs)
    op.setConfiguration(fs.getConf.asInstanceOf[HamaConfiguration]) 
    op
  }

}

class HDFS extends Operation {

  private var conf = new HamaConfiguration() // var conf must sit before var hdfs in case NPE
  protected var hdfs: FileSystem = FileSystem.get(configuration)
  private val localfs = HDFSLocal(FileSystem.getLocal(configuration))

  private[fs] def setConfiguration(conf: HamaConfiguration) = this.conf = conf

  override def configuration: HamaConfiguration = this.conf

  private[fs] def setFileSystem(fs: FileSystem) = this.hdfs = fs
 
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
  override def create(path: Path): OutputStream = {
    validate
    var out: OutputStream = null
    var created = false
    if(!exists(path)) 
      created = hdfs.createNewFile(path)
    else 
      created = true
    if(created) 
      out = hdfs.create(path)
    out 
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

/*
  TODO: scheme must be known beforehand so to decide which method to use.
  override def copy(from: Path)(to: Path) {
    val fromScheme = from.toUri.getScheme
    if(null == fromScheme) 
      throw new IllegalArgumentException("Unknown scheme for from: "+from)
    val toScheme = to.toUri.getScheme
    if(null == toScheme) 
      throw new IllegalArgumentException("Unknown scheme for to: "+to)

    //hdfs.copytToLocalFile(from, to)
  }
*/

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
  
  override def local: Operation = localfs

  override def operationFor(path: Path): Operation = {
    HDFS(path.getFileSystem(configuration))
  }

  override def makeQualified(path: Path): String = 
    path.makeQualified(hdfs).toString
}
