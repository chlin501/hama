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
package org.apache.hama.io;

import java.io.IOException;
import org.apache.hama.bsp.Counters;
import org.apache.hama.HamaConfiguration;

/**
 * A interface for input and output.
 */
public interface IO<I, O> {

  /**
   * Access to the input resource.
   * @return input resource to be accessed.
   */
  <I> I reader() throws IOException;
 
  /**
   * Access to the underlying output resource.
   * @return output resource to be accessed.
   */
  <O> O writer() throws IOException;

  /**
   * This denotes the split size to be processed.
   * @return long value of the split data size.
   */
  long splitSize();

  void initialize(HamaConfiguration taskConf, PartitionedSplit split, 
                  Counters counters);

}
