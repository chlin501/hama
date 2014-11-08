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
package org.apache.hama
/*
import org.apache.hama.conf.Setting
import org.apache.hama.util.Curator
import org.apache.zookeeper.CreateMode

object Registrator {

  def apply(setting: Setting): Registrator = new Registrator(setting)

}

class Registrator(setting: Setting) extends Curator {

  initializeCurator(setting.hama)
  protected val pattern = """(\w+)_(\w+)@(\w+):(\d+)""".r

  def masters(): Array[ProxyInfo] = list("/masters").map { child => {
    LOG.debug("Master znode found is {}", child)
    val ary = pattern.findAllMatchIn(child).map { m =>
      val name = m.group(1)
      val sys = m.group(2)
      val host = m.group(3)
      val port = m.group(4).toInt
      new ProxyInfo.MasterBuilder(name, setting.hama).build
    }.toArray
    ary(0)
  }}.toArray

  def register() {
    val sys = setting.info.getActorSystemName
    val host = setting.info.getHost
    val port = setting.info.getPort
    val path = "/%s/%s_%s@%s:%s".format("masters", setting.name, sys, host,
                                        port)
    create(path, CreateMode.EPHEMERAL)
  }

  def register() {
    val sys = setting.info.getActorSystemName
    val host = setting.info.getHost
    val port = setting.info.getPort
    val path = "/%s/%s_%s@%s:%s".format("masters", setting.name, sys, host,
                                        port)
    create(path, CreateMode.EPHEMERAL)
  }


}
*/
