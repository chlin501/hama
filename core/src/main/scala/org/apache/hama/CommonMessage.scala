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

sealed trait CommonMessage

/**
 * Notify when the system is in Normal state.
 * @param systemName of the server.
 */
final case class Ready(systemName: String) extends CommonMessage

/**
 * Notify when the servicde is in Stopped state.
 * @param systemName denotes the name of the system.
 */
final case class Halt(systemName: String) extends CommonMessage

/**
 * Inidicate timeout when looking up proxy with name and path specificed.
 * @param name is the name of the proxy
 * @param path is the path pointed to the target proxy.
 */
final case class Timeout(name: String, path: String) extends CommonMessage

/**
 * Signfy a close operation message.
 */
final case object Close extends CommonMessage
