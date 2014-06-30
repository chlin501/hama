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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.commons.io.IOUtils;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

public class SnappyCompressor extends BSPMessageCompressor {

  @Override
  public byte[] compress(byte[] bytes) {
    ByteArrayOutputStream bos = null;
    SnappyOutputStream sos = null;
    DataOutputStream dos = null;
    byte[] compressedBytes = null;

    try {
      bos = new ByteArrayOutputStream();
      sos = new SnappyOutputStream(bos);
      dos = new DataOutputStream(sos);

      dos.write(bytes);
      dos.close(); // Flush the stream as no more data will be sent.

      compressedBytes = bos.toByteArray();

    } catch (IOException ioe) {
      getLog().error("Unable to compress", ioe);
    } finally {
      try {
        sos.close();
        bos.close();
      } catch (IOException e) {
        getLog().warn("Failed to close compression streams.", e);
      }
    }
    return compressedBytes;
  }

  /**
   * Decompresses a BSPCompressedBundle and returns the corresponding
   * BSPMessageBundle.
   * 
   * @param compMsgBundle
   * @return
   */
  @Override
  public byte[] decompress(byte[] compressedBytes) {
    ByteArrayInputStream bis = null;
    SnappyInputStream sis = null;
    DataInputStream dis = null;
    byte[] bytes = null;

    try {
      bis = new ByteArrayInputStream(compressedBytes);
      sis = new SnappyInputStream(bis);
      dis = new DataInputStream(sis);

      bytes = IOUtils.toByteArray(dis);
    } catch (IOException ioe) {
      getLog().error("Unable to decompress.", ioe);
    } finally {
      try {
        dis.close();
        sis.close();
        bis.close();
      } catch (IOException e) {
        getLog().warn("Failed to close decompression streams.", e);
      }
    }

    return bytes;
  }

}
