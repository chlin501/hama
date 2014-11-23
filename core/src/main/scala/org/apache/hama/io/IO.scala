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
package org.apache.hama.io
/*
import java.io.IOException
import org.apache.hadoop.util.ReflectionUtils
import org.apache.hama.bsp.Counters
import org.apache.hama.HamaConfiguration
import org.apache.hama.bsp.FileSplit

object IO {

   * Only instantiate object without initialize to a specific task.
   * Need to call initialize(taskConf, split, counters) explicitly.
   * @param conf is common configuration without tight to any specific tasks.
   * @return IO is default to DefaultIO class.
  def get[I, O](conf: HamaConfiguration): IO[I, O] = {
    val clazz = conf.getClassByName(conf.get("bsp.io.class",
                                    classOf[DefaultIO].getCanonicalName))
    ReflectionUtils.newInstance(clazz, conf).asInstanceOf[IO[I, O]]
  }

}

 * A interface for input and output.
trait IO[I, O] {

   * Access to the input resource.
   * @return input resource to be accessed.
  @throws(classOf[IOException])
  def reader(): I 
 
   * Access to the underlying output resource.
   * @return output resource to be accessed.
  @throws(classOf[IOException])
  def writer(): O 

   * This denotes the split size to be processed.
   * @return long value of the split data size.
  def splitSize(): Long

   * Intialize io with corresponded task configuration, partitioned split, and
   * counters.
   * @param taskConf contains setting for a specific task.
   * @param split is partial data to be consumed by the corresponded.
   * @param counters is used for statistics.
  def initialize(taskConf: HamaConfiguration, split: FileSplit, 
                 counters: Counters)

}
*/
