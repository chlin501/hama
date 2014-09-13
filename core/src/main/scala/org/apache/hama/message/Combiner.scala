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

import org.apache.hadoop.io.Writable
import org.apache.hadoop.util.ReflectionUtils
import org.apache.hama.Constants
import org.apache.hama.HamaConfiguration
import scala.util.Try
import scala.util.Success
import scala.util.Failure

object Combiner {

  /**
   * Obtain Combiner based on common configuration passed in.
   * @param conf is common configuration, not specific to any task.
   * @return Option[Combiner[M]] is an option with type Combiner
   */
  def get[M <: Writable](conf: Option[HamaConfiguration]): Option[Combiner[M]] =
    conf match {
      case None => None
      case Some(found) => found.get(Constants.COMBINER_CLASS) match {
        case null => None
        case combinerName@_ => {
          val combinerClass = found.getClassByName(combinerName)
          newInstance[M](combinerClass, 
                         conf.getOrElse(new HamaConfiguration)) match {
            case Success(instance) => Some(instance)
            case Failure(cause) => None
          }
        }
      }
    }

  private def newInstance[M <: Writable](combinerClass: Class[_],
                                         conf: HamaConfiguration): 
    Try[Combiner[M]] = Try(ReflectionUtils.newInstance(combinerClass, conf).
                                           asInstanceOf[Combiner[M]])

}

abstract class Combiner[M <: Writable] {

  /**
   * Combines messages
   * 
   * @param messages
   * @return the combined message
   */
  def combine(messages: java.lang.Iterable[M]): M

}
