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
package org.apache.hama.logging

import org.apache.commons.logging.Log
import org.apache.hama.TestEnv
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

final case class InfoMsg(msg: String)
final case class DebugMsg(msg: String)
final case class WarnMsg(msg: String)
final case class ErrMsg(msg: String)

trait TestMsg extends LoggingAdapter {

  var infoMsg: InfoMsg = _
  var debugMsg: DebugMsg = _
  var warnMsg: WarnMsg = _
  var errMsg: ErrMsg = _

}

class MockCommonLogging(log: Log) extends CommonLogging(log) with TestMsg {

  override def info(msg: String, args: Any*) = 
    infoMsg = InfoMsg(format(msg, args:_*))

  override def debug(msg: String, args: Any*) = 
    debugMsg = DebugMsg(format(msg, args:_*))

  override def warning(msg: String, args: Any*) = 
    warnMsg = WarnMsg(format(msg, args:_*))

  override def error(msg: String, args: Any*) = 
    errMsg = ErrMsg(format(msg, args:_*))
 
}

class MockActorLogging(adptr: akka.event.LoggingAdapter) 
     extends ActorLogging(adptr) with TestMsg {
 
  override def info(msg: String, args: Any*) = 
    infoMsg = InfoMsg(format(msg, args:_*))

  override def debug(msg: String, args: Any*) = 
    debugMsg = DebugMsg(format(msg, args:_*))

  override def warning(msg: String, args: Any*) = 
    warnMsg = WarnMsg(format(msg, args:_*))

  override def error(msg: String, args: Any*) = 
    errMsg = ErrMsg(format(msg, args:_*))
}

@RunWith(classOf[JUnitRunner])
class TestLogging extends TestEnv("TestLogging") {

  val testMsg = "{}: test msg!"

  it("test logging mechanism.") {
    LOG.info("Test logging")
    val actorLog = new MockActorLogging(null)
    LOG.info("Test actor logging ...")
    assertFor(actorLog)
    val commonLog = new MockCommonLogging(null)
    LOG.info("Test common logging ...")
    assertFor(commonLog)
  }

  @throws(classOf[Exception]) 
  def assertFor(actorLog: TestMsg) {
    actorLog.info(testMsg, "info")
    val infoResult = actorLog.infoMsg.msg
    LOG.info("info log result: "+infoResult)
    assert("info: test msg!".equals(infoResult))

    actorLog.debug(testMsg, "debug")
    val debugResult = actorLog.debugMsg.msg
    LOG.info("debug log result: "+debugResult)
    assert("debug: test msg!".equals(debugResult))

    actorLog.warning(testMsg, "warning")
    val warnResult = actorLog.warnMsg.msg
    LOG.info("warning log result: "+warnResult)
    assert("warning: test msg!".equals(warnResult))

    actorLog.error(testMsg, "error")
    val errResult = actorLog.errMsg.msg
    LOG.info("error log result: "+errResult)
    assert("error: test msg!".equals(errResult))
  }

}
