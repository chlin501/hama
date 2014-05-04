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

import org.apache.hadoop.fs.Path
import org.apache.hama.HamaConfiguration

object Operation {

  val seperator: String = "/"

  def create(conf: HamaConfiguration): Operation = {
    val clazz = conf.getClass("bsp.fs.class", classOf[HDFS])
    var op: Operation = null
    if(classOf[HDFS].equals(clazz)) {
      op = HDFS(conf)
    } else {
      throw new UnsupportedOperationException("Operation for underlying "+
                                              clazz.getSimpleName+" not "+
                                              " yet supported.")
    }
    op
  }

}

trait Operation {

  def configuration: HamaConfiguration

  /**
   * Create a file based on the {@link Path} for writing.
   * @param path is the dirs to be created.
   * @return Boolean denotes if dirs are created or not.
   */
  @throws(classOf[IOException])
  def mkdirs(path: Path): Boolean 

  /**
   * Create a file based on the {@link Path} for writing.
   * @param path to be created.
   * @return OutputStream to which content will be written.
   */
  @throws(classOf[IOException])
  def create(path: Path): OutputStream

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

  /**
   * Copy file to local path.
   * @param from the source path the file to be copied. 
   * @param to the dest path the file to be copied.
  @throws(classOf[IOException])
  def copy(from: Path)(to: Path)
   */

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
   * Obtain operation for local file system.
   * @return operation for local file system. 
   */
  def local: Operation

  /**
   * Obtain an operation that owns the given path.
   * @return Operation for a particular path supplied.
   */
  def operationFor(path: Path): Operation
}