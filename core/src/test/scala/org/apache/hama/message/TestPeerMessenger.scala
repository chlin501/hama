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

import akka.actor.ActorRef
import akka.actor.ActorSystem
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import java.util.concurrent.ExecutorService
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.Writable
import org.apache.hama.HamaConfiguration
import org.apache.hama.message.compress.BSPMessageCompressor
import org.apache.hama.TestEnv
import org.apache.hama.logging.Logger
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import scala.collection.JavaConversions._

class RemotePeerMessenger(tester: ActorRef) extends PeerMessenger { 

  override def receive = super.receive

}

object LocalPeerMessenger {

  val dummyPeer = Peer.at("TestPeerMessenger", "127.0.0.1", 1234)

}

class LocalPeerMessenger(tester: ActorRef) extends PeerMessenger {

  import LocalPeerMessenger._

//  val remoteName: String = "remotePeer"
  //val remoteAddr: String = "akka://TestPeerMessenger/user/remotePeer"
  // this should be quals to the one in testing function.

  override def link(target: String, ref: ActorRef): ActorRef = {
    LOG.info("[For test only] Link to remote target: {} ref: {}.", target, ref)
    val proxy = ref
    proxies ++= Set(proxy)
    proxiesLookup.get(target) match {
      case Some(cancellable) => cancellable.cancel
      case None =>
        LOG.warning("Can't cancel for proxy {} not found!", target)
    }
    LOG.info("Done linking to remote service {}.", target)
    proxy
  }

  override def afterLinked(target: String, proxy: ActorRef) {
    LOG.info("Replace peer messenger path with {}.", dummyPeer.getPath)
    findThenSend(dummyPeer.getPath, proxy)
  }

  override def lookupPeer(name: String, addr: String) = {
    LOG.info("Lookup local peer instead of remote - name {} at {}", name, addr)
    //LOG.info("Lookup local peer instead of remote - name {} at {}", 
             //remoteName, remoteAddr)
    lookup(name, addr)
    //lookup(remoteName, remoteAddr)
  }

  override def receive = super.receive

}

@RunWith(classOf[JUnitRunner])
class TestPeerMessenger extends TestEnv(ActorSystem("TestPeerMessenger")) {

  val localMsgQueue = new LinkedBlockingQueue[BSPMessageBundle[Writable]]()
  val executor = Executors.newSingleThreadExecutor()
  val seq = Seq[IntWritable](new IntWritable(1), new IntWritable(99), 
                             new IntWritable(23))
  var forVerification: BSPMessageBundle[IntWritable] = _

  class MessageConsumer extends Callable[Boolean] with Logger {
    override def call(): Boolean = {
      var flag = true
      while(!Thread.currentThread().isInterrupted() && flag) {
        LOG.info("Start waiting for the incoming message ... ")
        val bundle = localMsgQueue.take
        if(null == bundle) 
          throw new RuntimeException("Msg bundle in queue is null!")
        forVerification = bundle.asInstanceOf[BSPMessageBundle[IntWritable]]
        if(null != forVerification) flag = false
      }
      LOG.info("Escape while loop with flag set to "+flag)
      true
    } 
  }

  override def beforeAll {
    super.beforeAll
    executor.submit(new MessageConsumer())
  }

  override def afterAll {
    executor.shutdown
    super.afterAll
  }

  def createPeer[M <: Writable](name: String,
                                queue: BlockingQueue[BSPMessageBundle[M]],
                                clazz: Class[_]): ActorRef = { 
    val actor = createWithArgs(name, clazz, tester)
    actor ! Setup(testConfiguration, queue)
    actor
  }

  def createBundle[M <: Writable](msgs: M*): BSPMessageBundle[M] = {
    val threshold = 
      testConfiguration.getLong("hama.messenger.compression.threshold", 128)
    val bundle = new BSPMessageBundle[M]()
    bundle.setCompressor(BSPMessageCompressor.get(testConfiguration), threshold)
    msgs.foreach( msg => bundle.addMessage(msg))
    bundle
  }

  it("test peer messenger function.") {

    val localPeer = createPeer[Writable]("localPeer", 
                                         localMsgQueue, 
                                         classOf[LocalPeerMessenger])
    val remotePeer = createPeer[Writable]("remotePeer", 
                                          localMsgQueue,
                                          classOf[RemotePeerMessenger])
    val bundle = createBundle[IntWritable](seq(0), seq(1), seq(2))
    localPeer ! Transfer(LocalPeerMessenger.dummyPeer, bundle)
    Thread.sleep(15*1000)
    LOG.info("Bundle "+forVerification+" size "+forVerification.size)
    assert(null != forVerification)
    var idx = 0
    asScalaIterator(forVerification.iterator).foreach( e => {
      assert(null != e)
      LOG.info("Found a message at index "+idx+" with value "+e.get)
      assert(e.get == seq(idx).get)
      idx+=1
    })
    LOG.info("Messages sent: "+bundle.size+". Messages received: "+idx)
  }
}
