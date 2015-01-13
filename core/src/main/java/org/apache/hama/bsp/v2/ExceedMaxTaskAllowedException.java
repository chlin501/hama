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
package org.apache.hama.bsp.v2;

public final class ExceedMaxTaskAllowedException extends RuntimeException {

  final String jobId;
  final int maxAttemptAllowed;

  public ExceedMaxTaskAllowedException(final String jobId,
                                       final int maxAllowed) {
    this.jobId = jobId;
    this.maxAttemptAllowed = maxAllowed;
  }

  public String getJobId() {
    return jobId;
  }

  public int getMaxAttemptAllowed() {
    return maxAttemptAllowed;
  }

  @Override 
  public String toString() {
    return "Exceed "+getMaxAttemptAllowed()+" max tasks allowed "+
           "for job "+getJobId();
  }

}
