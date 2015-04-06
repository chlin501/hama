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
package org.apache.hama.io.serialization

import akka.actor.ExtendedActorSystem
import akka.serialization.Serializer
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream
import java.util.concurrent.Callable
import org.apache.hadoop.io.Writable
import org.apache.hama.logging.CommonLog
import org.apache.hama.io.serialization.WritableSerializer.CurrentSystem
import scala.util.DynamicVariable
import scala.util.Failure
import scala.util.Success
import scala.util.Try

object WritableSerializer {

  val currentSystem = new CurrentSystem
 
  final class CurrentSystem extends DynamicVariable[ExtendedActorSystem](null) {

    def withValue[S](value: ExtendedActorSystem, callable: Callable[S]): S = 
      super.withValue[S](value)(callable.call)

  }
}

class WritableSerializer(val system: ExtendedActorSystem) extends Serializer 
                                                          with CommonLog {

  override def includeManifest: Boolean = true
 
  override def identifier = 129452 

  override def toBinary(any: AnyRef): Array[Byte] = {
    if(!any.isInstanceOf[Writable]) 
      throw new RuntimeException("Can't serialize "+any.getClass.getName)
    val writable = any.asInstanceOf[Writable]
    val bout = new ByteArrayOutputStream()
    val out = new DataOutputStream(bout)
    try {
      WritableSerializer.currentSystem.withValue(system) { 
        writable.write(out)
      }
    } finally {
      out.close 
    }
    bout.toByteArray
  }

  // TODO: instantiate with dynamic constructor?
  protected def initialize(bytes: Array[Byte], clz: Class[_]): Writable = {
    val bin = new ByteArrayInputStream(bytes)
    val in = new DataInputStream(bin) 
    val cls = Class.forName(clz.getName)
    LOG.debug("Instantiate class {} ...", cls)
    val instance = cls.newInstance
    if(!instance.isInstanceOf[Writable])   
      throw new RuntimeException("Not Writable class "+clz.getName+"!")
    var writable: Writable = null
    WritableSerializer.currentSystem.withValue(system) { 
      writable = instance.asInstanceOf[Writable]
      writable.readFields(in)
    }
    try {} finally { in.close }
    writable
  }

  override def fromBinary(bytes: Array[Byte], clazz: Option[Class[_]]): 
    AnyRef = clazz match {
      case Some(clz) => Try(initialize(bytes, clz)) match {
        case Success(writable) => writable
        case Failure(cause) => {
          LOG.error("Fail instantiating {} because {}", clz.getName, cause)
          throw cause
        }
      }
      case None => throw new ClassNotFoundException("Class not found!")
    }
}
