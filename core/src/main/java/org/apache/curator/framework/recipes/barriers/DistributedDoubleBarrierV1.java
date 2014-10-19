/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.curator.framework.recipes.barriers;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.hama.bsp.TaskAttemptID;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class DistributedDoubleBarrierV1
{

    static final Log LOG = LogFactory.getLog(DistributedDoubleBarrierV1.class);

    private final CuratorFramework client;
    private final String barrierPath;
    private final TaskAttemptID taskAttemptId; 
    private final int memberQty;
    private final AtomicBoolean hasBeenNotified = new AtomicBoolean(false);
    private final AtomicBoolean connectionLost = new AtomicBoolean(false);
    private final Watcher watcher = new Watcher()
    {
        @Override
        public void process(WatchedEvent event)
        {
            connectionLost.set(event.getState() != Event.KeeperState.SyncConnected);
            notifyFromWatcher();
        }
    };

    private static final String     READY_NODE = "ready";
    
    /**
     * Customized distributed double barrier, based on curator's 
     * {@link DistributedDoubleBarrier}.
     * @param client is a started curator framework instance.
     * @param rootPath is the root path where all peers will synchronize.
     * @param taskAttemptId is the task attempt id held by the peer.
     * @param memberQty is the number of bsp peers will meet at the rendezvous 
     *                  points.
     */
    public DistributedDoubleBarrierV1(final CuratorFramework client, 
                                      final String rootPath, 
                                      final TaskAttemptID taskAttemptId, 
                                      final int memberQty) {
        Preconditions.checkState(memberQty > 0, "memberQty cannot be 0");

        this.client = client;
        this.barrierPath = 
          ZKPaths.makePath(rootPath, taskAttemptId.getJobID().toString());
        this.taskAttemptId = taskAttemptId;
        this.memberQty = memberQty;
    }

    protected String getParentPath(long superstep) {
      return ZKPaths.makePath(barrierPath, new Long(superstep).toString());
    }

    protected String getOurPath(long superstep) {
      return ZKPaths.makePath(getParentPath(superstep), 
                              taskAttemptId.toString());
    }
  
    protected String getReadyPath(long superstep) {
      return ZKPaths.makePath(getParentPath(superstep), READY_NODE);
    }

    /**
     * Enter the barrier and block until all members have entered
     *
     * @throws Exception interruptions, errors, etc.
     */
    public void     enter(long superstep) throws Exception
    {
        enter(superstep, -1, null);
    }

    /**
     * Enter the barrier and block until all members have entered or the timeout has
     * elapsed
     *
     * @param maxWait max time to block
     * @param unit time unit
     * @return true if the entry was successful, false if the timeout elapsed first
     * @throws Exception interruptions, errors, etc.
     */
    public boolean     enter(long superstep, long maxWait, TimeUnit unit) throws Exception
    {
        long            startMs = System.currentTimeMillis();
        boolean         hasMaxWait = (unit != null);
        long            maxWaitMs = hasMaxWait ? TimeUnit.MILLISECONDS.convert(maxWait, unit) : Long.MAX_VALUE;
        final String ourPath = getOurPath(superstep);
        final String readyPath = getReadyPath(superstep);
        boolean         readyPathExists = (client.checkExists().usingWatcher(watcher).forPath(readyPath) != null);
        client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(ourPath);
        LOG.debug("Task "+taskAttemptId.toString()+" in enter function with ourPath: "+ourPath+", readyPath: "+readyPath+", and readyPathExists: "+readyPathExists);
        boolean         result = (readyPathExists || internalEnter(superstep, startMs, hasMaxWait, maxWaitMs));
        if ( connectionLost.get() )
        {
            throw new KeeperException.ConnectionLossException();
        }

        return result;
    }

    /**
     * Leave the barrier and block until all members have left
     *
     * @throws Exception interruptions, errors, etc.
     */
    public synchronized void     leave(long superstep) throws Exception
    {
        leave(superstep, -1, null);
    }

    /**
     * Leave the barrier and block until all members have left or the timeout has
     * elapsed
     *
     * @param maxWait max time to block
     * @param unit time unit
     * @return true if leaving was successful, false if the timeout elapsed first
     * @throws Exception interruptions, errors, etc.
     */
    public synchronized boolean     leave(long superstep, long maxWait, TimeUnit unit) throws Exception
    {
        long            startMs = System.currentTimeMillis();
        boolean         hasMaxWait = (unit != null);
        long            maxWaitMs = hasMaxWait ? TimeUnit.MILLISECONDS.convert(maxWait, unit) : Long.MAX_VALUE;
        return internalLeave(superstep, startMs, hasMaxWait, maxWaitMs);
    }

    @VisibleForTesting
    protected List<String> getChildrenForEntering(long superstep) throws Exception
    {
        
        return client.getChildren().forPath(getParentPath(superstep));
    }

    private List<String> filterAndSortChildren(List<String> children)
    {
        Iterable<String> filtered = Iterables.filter
        (
            children,
            new Predicate<String>()
            {
                @Override
                public boolean apply(String name)
                {
                    return !name.equals(READY_NODE);
                }
            }
        );

        ArrayList<String> filteredList = Lists.newArrayList(filtered);
        Collections.sort(filteredList);
        return filteredList;
    }

    private boolean internalLeave(long superstep, long startMs, boolean hasMaxWait, long maxWaitMs) throws Exception
    {
        String          ourPathName = ZKPaths.getNodeFromPath(getOurPath(superstep));
        boolean         ourNodeShouldExist = true;
        boolean         result = true;
        LOG.debug("Task "+taskAttemptId.toString()+" in internalLeave with ourPathName: "+ourPathName);
        for(;;)
        {
            if ( connectionLost.get() )
            {
                throw new KeeperException.ConnectionLossException();
            }

            List<String> children;
            try
            {
                children = client.getChildren().forPath(getParentPath(superstep));
            }
            catch ( KeeperException.NoNodeException dummy )
            {
                children = Lists.newArrayList();
            }
            children = filterAndSortChildren(children);
            if ( (children == null) || (children.size() == 0) )
            {
                break;
            }

            int                 ourIndex = children.indexOf(ourPathName);
            if ( (ourIndex < 0) && ourNodeShouldExist )
            {
                if ( connectionLost.get() )
                {
                    break;  // connection was lost but we've reconnected. However, our ephemeral node is gone
                }
                else
                {
                    throw new IllegalStateException(String.format("Our path (%s) is missing", ourPathName));
                }
            }

            if ( children.size() == 1 )
            {
                if ( ourNodeShouldExist && !children.get(0).equals(ourPathName) )
                {
                    throw new IllegalStateException(String.format("Last path (%s) is not ours (%s)", children.get(0), ourPathName));
                }
                checkDeleteOurPath(superstep, ourNodeShouldExist);
                break;
            }

            Stat            stat;
            boolean         IsLowestNode = (ourIndex == 0);
            if ( IsLowestNode )
            {
                String  highestNodePath = ZKPaths.makePath(barrierPath, children.get(children.size() - 1));
                stat = client.checkExists().usingWatcher(watcher).forPath(highestNodePath);
            }
            else
            {
                String  lowestNodePath = ZKPaths.makePath(barrierPath, children.get(0));
                stat = client.checkExists().usingWatcher(watcher).forPath(lowestNodePath);

                checkDeleteOurPath(superstep, ourNodeShouldExist);
                ourNodeShouldExist = false;
            }

            if ( stat != null )
            {
                if ( hasMaxWait )
                {
                    long        elapsed = System.currentTimeMillis() - startMs;
                    long        thisWaitMs = maxWaitMs - elapsed;
                    if ( thisWaitMs <= 0 )
                    {
                        result = false;
                    }
                    else
                    {
                        wait(thisWaitMs);
                    }
                }
                else
                {
                    wait();
                }
            }
        }

        try
        {
            client.delete().forPath(getReadyPath(superstep));
        }
        catch ( KeeperException.NoNodeException ignore )
        {
            // ignore
        }

        return result;
    }

    private void checkDeleteOurPath(long superstep, boolean shouldExist) throws Exception
    {
        if ( shouldExist )
        {
            client.delete().forPath(getOurPath(superstep));
        }
    }

    private synchronized boolean internalEnter(long superstep, long startMs, boolean hasMaxWait, long maxWaitMs) throws Exception
    {
        boolean result = true;
        do
        {
            List<String>    children = getChildrenForEntering(superstep);
            LOG.debug("Task "+taskAttemptId.toString()+" discovers children size: "+children.size());
            int             count = (children != null) ? children.size() : 0;
            if ( count >= memberQty )
            {
                try
                {
                    LOG.debug("Task "+taskAttemptId.toString()+" tries creating ready znode at "+getReadyPath(superstep));
                    client.create().forPath(getReadyPath(superstep));
                }
                catch ( KeeperException.NodeExistsException ignore )
                {
                    // ignore
                }
                break;
            }

            if ( hasMaxWait && !hasBeenNotified.get() )
            {
                long        elapsed = System.currentTimeMillis() - startMs;
                long        thisWaitMs = maxWaitMs - elapsed;
                if ( thisWaitMs <= 0 )
                {
                    result = false;
                }
                else
                {
                    wait(thisWaitMs);
                }

                if ( !hasBeenNotified.get() )
                {
                    result = false;
                }
            }
            else
            {
                LOG.debug("Task "+taskAttemptId.toString()+" is going to wait!");
                wait();
            }
        } while ( false );

        return result;
    }

    private synchronized void notifyFromWatcher()
    {
        hasBeenNotified.set(true);
        notifyAll();
    }
}
