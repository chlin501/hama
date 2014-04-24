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
package org.apache.hama.util

import java.nio.ByteBuffer

trait Conversion {

  protected def toInt(data: Array[Byte]): Int = ByteBuffer.wrap(data).getInt

  protected def toFloat(data: Array[Byte]): Float =
    ByteBuffer.wrap(data).getFloat

  protected def toLong(data: Array[Byte]): Long =
    ByteBuffer.wrap(data).getLong

  protected def toDouble(data: Array[Byte]): Double =
    ByteBuffer.wrap(data).getDouble

  protected def toBytes(data: Int): Array[Byte] =
    ByteBuffer.allocate(4).putInt(data).array

  protected def toBytes(data: Float): Array[Byte] =
    ByteBuffer.allocate(4).putFloat(data).array

  protected def toBytes(data: Long): Array[Byte] =
    ByteBuffer.allocate(8).putLong(data).array

  protected def toBytes(data: Double): Array[Byte] =
    ByteBuffer.allocate(8).putDouble(data).array

  protected def toBytes(data: String): Array[Byte] = data.getBytes

}
