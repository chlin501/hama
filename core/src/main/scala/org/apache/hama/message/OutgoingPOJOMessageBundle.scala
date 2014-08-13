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
package org.apache.hama.message

import java.util.concurrent.ConcurrentHashMap
import java.util.Iterator
import java.util.Map.Entry

import org.apache.hadoop.io.Writable
import org.apache.hama.bsp.Combiner
import org.apache.hama.Constants
import org.apache.hama.HamaConfiguration
import org.apache.hama.message.compress.BSPMessageCompressor
import org.apache.hama.ProxyInfo
import org.apache.hama.util.ReflectionUtils
import org.apache.hama.logging.Logger
import scala.util.Try
import scala.util.Success
import scala.util.Failure

class OutgoingPOJOMessageBundle[M <: Writable] 
      extends OutgoingMessageManager[M] with Logger {

  type PeerAndBundleIter = Iterator[Entry[ProxyInfo, BSPMessageBundle[M]]] 

  private var conf: Option[HamaConfiguration] = None
  private var compressor: Option[BSPMessageCompressor] = None
  private var combiner: Option[Combiner[M]] = None
  private val outgoingBundles = 
    new ConcurrentHashMap[ProxyInfo, BSPMessageBundle[M]]() 

  override def init(conf: HamaConfiguration, 
                    compressor: BSPMessageCompressor) {
    this.conf = Some(conf)
    this.compressor = Some(compressor)
    this.combiner = getCombiner(this.conf)
  }

  protected def getCombiner(conf: Option[HamaConfiguration]): 
    Option[Combiner[M]] = conf match {
      case None => None
      case Some(found) => {
        found.get(Constants.COMBINER_CLASS) match {
          case null => None
          case combinerName@_ => {
            val combinerClass = found.getClassByName(combinerName)
            newInstance(combinerClass) match {
              case Success(instance) => Some(instance)
              case Failure(cause) => {
                LOG.error("Fail instantiating "+combinerName, cause)
                None
              }
            }
          }
        }
      }
    } 

  protected def newInstance(combinerClass: Class[_]): Try[Combiner[M]] =
    Try(ReflectionUtils.newInstance(combinerClass).asInstanceOf[Combiner[M]])
  

  protected def getCompressionThreshold(conf: Option[HamaConfiguration]): 
    Long = conf match {
      case None => 128L
      case Some(found) => found.getLong("hama.messenger.compression.threshold",
                                        128)
  }

  override def addMessage(peer: ProxyInfo, msg: M) {
    outgoingBundles.containsKey(peer) match {
      case true => 
      case false => {
        compressor match {
          case None =>
          case Some(found) => {
            val bundle = new BSPMessageBundle[M]()
            bundle.setCompressor(found, getCompressionThreshold(this.conf))
            outgoingBundles.put(peer, bundle)
          }
        }
      }
    } 
    combiner match {
      case None => outgoingBundles.get(peer).addMessage(msg)
      case Some(found) => {
        compressor match {
          case None =>
          case Some(f) => {
            val bundle = outgoingBundles.get(peer)
            bundle.addMessage(msg)
            val combined = new BSPMessageBundle[M]()
            combined.setCompressor(f, getCompressionThreshold(this.conf))
            combined.addMessage(found.combine(bundle))
            outgoingBundles.put(peer, combined)
          }
        }
      }
    }
  }

  override def clear() = outgoingBundles.clear

  override def getBundleIterator(): java.util.Iterator[java.util.Map.Entry[ProxyInfo, BSPMessageBundle[M]]] = outgoingBundles.entrySet.iterator 

}
