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
package org.apache.hama

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TestHamaConfiguration extends TestEnv("TestHamaConfiguration") {

  it("test hama configuration constructor.") {
    val loader = Thread.currentThread.getContextClassLoader
    val conf = new HamaConfiguration()   
    conf.setClassLoader(loader)
    val cloned = new HamaConfiguration(conf)
    val clonedLoader = cloned.getClassLoader()
    LOG.info("Cloned class loader ({}) and origin class loader {}", 
             clonedLoader, loader)
    assert(clonedLoader.equals(loader))

    val newLoader = new java.net.URLClassLoader(Array[java.net.URL](new java.io.File("/tmp/").toURI.toURL))
    conf.setClassLoader(newLoader)
    val clonedLoader1 = cloned.getClassLoader()
    LOG.info("cloned loader 1: {}, clonded loader: {}, origin: {}, "+
             "new Loader: {}", clonedLoader1, clonedLoader, loader, newLoader) 
    assert(!clonedLoader1.equals(newLoader))
  }
}
