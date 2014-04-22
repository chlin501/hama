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
package org.apache.hama.fs;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hama.HamaConfiguration;

public final class MockFileSystem extends LocalFileSystem {

  final Log LOG = LogFactory.getLog(MockFileSystem.class);
  final HamaConfiguration conf;

  /**
   * {@link org.apache.hama.HamaConfiguration} is from {@link TestEnv}.
   */
  public MockFileSystem(final HamaConfiguration conf) { 
    this.conf = conf;
    if(null == this.conf)
      throw new IllegalArgumentException("HamaConfiguration is missing!");
  }

  @Override
  public long getDefaultBlockSize() {
    return conf.getLong("fs.local.block.size", 1024); //32*1024*1024
  }
}
