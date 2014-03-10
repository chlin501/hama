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

sealed trait State

/**
 * The system in the stage where services are starting up.
 */
private[hama] case object StartUp extends State 

/**
 * The system services are ready.
 */
private[hama] case object Normal extends State

/**
 * The system services are shutting down.
 */
private[hama] case object CleanUp extends State

/**
 * The system is stopped.
 */
private[hama] case object Stopped extends State

/**
 * The system is failed.
 */
private[hama] case object Failed extends State
