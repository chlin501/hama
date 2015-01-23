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
import java.io.DataOutputStream
import java.io.FileNotFoundException
import java.io.IOException
import java.io.InputStream
import java.io.OutputStream

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.ReflectionUtils
import org.apache.hama.HamaConfiguration

object Operation {

  /**
   * Create sys dir with permission set to rwx-wx-wx
   */
  val sysDirPermission = FsPermission.createImmutable(0733.asInstanceOf[Short])

  /**
   * Create job dir with permission set to rwx-rwx-rwx, i.e., global readable, 
   * writable, and executable.
   */
  val jobDirPermission = FsPermission.createImmutable(0777.asInstanceOf[Short])

  /**
   * Create job file with permission set to rw-r--r--; i.e., global readable, 
   * and owner writable.
   */
  val jobFilePermission = FsPermission.createImmutable(0644.asInstanceOf[Short])


  val seperator: String = "/"

  /**
   * Obtain file system Operation object.
   * @param conf is common configuration.
   * @return Operation for a specific file system, usually HDFS.
   */
  def get(conf: HamaConfiguration): Operation = get[HDFS](conf, classOf[HDFS])

  def get[F <: Operation](conf: HamaConfiguration, default: Class[F]): 
      Operation = {
    val clazz = conf.getClass("bsp.fs.class", default, classOf[Operation])
    val op = ReflectionUtils.newInstance(clazz, conf)
    op.initialize(conf)
    op
  }

  /**
   * Obtain default working directory provided with configuration.
   * @param conf contains setting for particular file system operation.
   */ 
  def defaultWorkingDirectory(conf: HamaConfiguration): String = {
    var workDir = conf.get("bsp.working.dir")
    workDir match {
      case null => {
        val fsDir = Operation.get(conf).getWorkingDirectory
        conf.set("bsp.working.dir", fsDir.toString)
        workDir = fsDir.toString
      }
      case _ =>
    }
    workDir
  }

  /**
   * Obtain an operation that owns the given path.
   * The effect should be the same as 
   * <code>
   *   Path.getFileSystem(configuration)
   * </code>
   * @return Operation for a particular path supplied.
   */
  def operationFor(path: Path, conf: HamaConfiguration): Operation = {
    val fs = get(conf); fs.setFs(path.getFileSystem(conf)); fs
  }

  @throws(classOf[IOException])
  def toDataOutputStream(out: OutputStream): DataOutputStream = 
    out.isInstanceOf[DataOutputStream] match {
      case true => out.asInstanceOf[DataOutputStream]
      case false => throw new IOException("Underlying is not DataOutputStream!")
    }

}

trait Operation {

  def initialize(conf: HamaConfiguration) 

  def configuration: HamaConfiguration

  protected[fs] def setFs(fs: FileSystem)

  /**
   * Make directory given with the {@link Path}.
   * @param path is the dirs to be created.
   * @return Boolean denotes if dirs are created or not.
   */
  @throws(classOf[IOException])
  def mkdirs(path: Path): Boolean 

  /**
   * Make directory given with the {@link Path} and permission.
   * @param path is the dirs to be created.
   * @param permission value to be assigned to the dir.
   * @return Boolean denotes if dirs are created or not.
   */  
  @throws(classOf[IOException])
  def mkdirs(path: Path, pemission: FsPermission): Boolean 

  /**
   * Create path with permission specified.
   * Note that currently hadoop's FileSystem#setPermission is not implemented. 
   * @param path is the path to be created.
   * @param permission specifies the permission for the path created.
   * @return OutputStream to which content will be written.
   */
  @throws(classOf[IOException])
  def create(path: Path, pemission: FsPermission): OutputStream

  /**
   * Create data based on the {@link Path} given.
   * @param path to be created.
   * @param overwrite the file at path; default to true.
   * @return OutputStream to which content will be written.
   */
  @throws(classOf[IOException])
  def create(path: Path, overwrite: Boolean = true): OutputStream

  /**
   * Create data based on the {@link Path} given with replication and block 
   * size suppplied.
   * @param path to be created.
   * @param replication vlaue to be used.
   * @param blockSize to be created.
   * @param overwrite the file at path; default to true.
   * @return OutputStream to which content will be written.
   */
  @throws(classOf[IOException])
  def create(path: Path, replication: Short, blockSize: Long, 
             overwrite: Boolean): OutputStream 

  /**
   * Append data for writing based on {@link Path}.
   * @param path to which the data will be appended. 
   * @return OutputStream is the target place where data to be written.
   */
  @throws(classOf[IOException])
  def append(path: Path): OutputStream

  /**
   * Open a file based on the {@link Path} for reading.
   * @param path to be created.
   * @return InputStream from which content will be read.
   */
  @throws(classOf[IOException])
  def open(path: Path): InputStream

  /**
   * Check if the path exists.
   * @param path will be checked if it's dir or not.
   * @return Boolean denotes if the path is dir or not.
   */  
  @throws(classOf[IOException])
  def exists(path: Path): Boolean

  /**
   * Delete a file.
   * @param path to be deleted.
   * @return Boolean denotes if the path is deleted or not.
   */
  @throws(classOf[IOException])
  def remove(path: Path): Boolean

  /**
   * Move a file from {@link Path} to {@link Path}.
   * @param from the path the original data is stored.
   * @param to the dest path the data will be stored.
   */  
  @throws(classOf[IOException])
  def move(from: Path, to: Path)

  /**
   * List files status.
   * @return List contains a list of files.
   */
  @throws(classOf[FileNotFoundException])
  @throws(classOf[IOException])
  def list[T](path: Path): java.util.List[T]

  @throws(classOf[IOException])
  def copyToLocal(from: Path)(to: Path)

  @throws(classOf[IOException])
  def copyFromLocal(from: Path)(to: Path)

  /**
   * Retrieve system directory.
   * @return Path of the system directory.
   */
  def getSystemDirectory: Path

  /**
   * Obtain file system's working directory.
   * @return Path of underlying file system's working directory.
   */  
  def getWorkingDirectory: Path 

  /**
   * Obtain operation for local file system.
   * @return operation for local file system. 
   */
  def local: Operation
  
  /**
   * Return a qualified path object, same as Path.makeQualified(FileSystem).
   * @param path to be normalized.
   */
  def makeQualified(path: Path): String

  /**
   * Change file system operation based on a particular working directory.
   * @param path is the working directory to be used.
   */
  def setWorkingDirectory(path: Path)

  /**
   * Close underlying output stream.
   * @param 
   */ 
  def close(stream: Closeable)
  
}
