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
package org.apache.hama.message.compress;

import org.apache.hadoop.util.ReflectionUtils
import org.apache.hama.HamaConfiguration

object BSPMessageCompressor {

  /**
   * Obtain compressor for bsp message(s).
   * @param conf is common configuration.
   * @return BSPMessageCompressor for compressing messages; return null if no 
   *                              compression class is set.
   */
  def get(conf: HamaConfiguration): BSPMessageCompressor = {
    var compressor: BSPMessageCompressor = null
    conf.get("hama.messenger.compression.class") match {
      case null =>
      case _ => {
       val name = conf.get("hama.messenger.compression.class", 
                           classOf[SnappyCompressor].getCanonicalName())
        val clazz = conf.getClassByName(name)
        compressor = ReflectionUtils.newInstance(clazz, conf).
                                     asInstanceOf[BSPMessageCompressor]
      }
    }
    compressor
  }

  def threshold(conf: Option[HamaConfiguration]): Long = conf match {
    case None => 128L
    case Some(found) => found.getLong("hama.messenger.compression.threshold",
                                      128)
  }

}

/**
 * Provides utilities for compressing and decompressing byte array.
 */
abstract class BSPMessageCompressor {

  type Compressed = Array[Byte]
  type Uncompressed = Array[Byte]

  /**
   * Compress raw bytes to compressed ones.
   * @param uncompressed byte array is the original data in the form of bytes.
   * @return bytes as array with data compressed.
   */
  def compress(uncompressed: Uncompressed): Compressed 

  /**
   * Decompress data into original (uncompressed state) bytes array.
   * @param compressed byte array is the compressed data.
   * @return bytes as array with data decompressed.
   */
  def decompress(compressed: Compressed): Uncompressed 
}
