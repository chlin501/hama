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

/*
import akka.actor._
import akka.event._
import akka.testkit._
import java.io._
import org.apache.commons.io.FileUtils
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.permission._
import org.apache.hama._
import org.apache.hama.fs._
import org.apache.hama.groom._
import org.apache.hama.bsp.BSPJobID
import org.apache.hama.bsp.v2._
import org.apache.hama.bsp.v2.IDCreator._
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

class MockStorage(conf: HamaConfiguration) extends Storage(conf) {

  override val LOG = Logging(context.system, this)

  val fs = new MockFileSystem(conf)

  override def fileSystem: FileSystem = fs
  override def localFs: FileSystem = fs

  // unit test only
  override def copyJobFile(jobId: BSPJobID, jobFile: String, 
                           localJobFile: String) = {
    LOG.info("Override copyJobFile jobId: {}, jobFile: {}, localJobFile: {}", 
             jobId, jobFile, localJobFile)
    FileUtils.copyFile(new File(jobFile), new File(localJobFile))
  }

  // unit test only
  override def copyJarFile(jobId: BSPJobID, jarFile: Option[String],
                           localJarFile: String) {
    val jar = jarFile.getOrElse(jobId.toString+".jar")
    LOG.info("Override copyJarFile jobId: {}, jarFile: {}, localJarFile: {}", 
             jobId, jar, localJarFile)
    // do nothing first
    // jar file may need to runtime compile, zip, then copy can be done!
    //FileUtils.copyFile(new File(jar), new File(localJarFile))
  }
  
  //override def receive = super.receive
}
*/
