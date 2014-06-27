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
/*
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hama.HamaConfiguration;

public class OperationFactory {

  static final Log LOG = LogFactory.getLog(OperationFactory.class);

  public static final String seperator = "/";

  @SuppressWarnings("rawtypes")
  public static Operation get(final HamaConfiguration conf) {
    final Class<?> clazz = 
      conf.getClass("bsp.fs.class", HDFS.class);
    Operation op = null;
    if(HDFS.class.equals(clazz)) {
      if(LOG.isDebugEnabled()) LOG.debug("Instantiate "+clazz.getName()+"...");
      final HDFS hdfs = new HDFS();
      hdfs.setConfiguration(conf);
      op = hdfs;
    } else if(HDFSLocal.class.equals(clazz)) {
      if(LOG.isDebugEnabled()) LOG.info("Instantiate "+clazz.getName()+"...");
      final HDFS hdfs = new HDFS();
      hdfs.setConfiguration(conf);
      op = hdfs.local();
    } else {
      throw new UnsupportedOperationException("Operation for underlying "+
                                              clazz.getName()+" not yet "+
                                              "supported.");
    }
    if(LOG.isDebugEnabled()) LOG.debug("Operation is configured to "+op);
    return op;
  }
}
*/
