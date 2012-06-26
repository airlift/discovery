/*
 * Copyright 2010 Proofpoint, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.proofpoint.discovery.store;

import com.proofpoint.configuration.Config;
import com.proofpoint.units.Duration;
import com.proofpoint.units.MinDuration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.concurrent.TimeUnit;

public class StoreConfig
{
    private Duration tombstoneMaxAge = new Duration(1, TimeUnit.DAYS);
    private Duration garbageCollectionInterval = new Duration(1, TimeUnit.HOURS);
    private int maxBatchSize = 1000;
    private int queueSize = 1000;
    private Duration remoteUpdateInterval = new Duration(5, TimeUnit.SECONDS);
    private Duration replicationInterval = new Duration(1, TimeUnit.MINUTES);

    @NotNull
    public Duration getTombstoneMaxAge()
    {
        return tombstoneMaxAge;
    }

    @Config("store.tombstone-max-age")
    public StoreConfig setTombstoneMaxAge(Duration age)
    {
        this.tombstoneMaxAge = age;
        return this;
    }

    @NotNull
    public Duration getGarbageCollectionInterval()
    {
        return garbageCollectionInterval;
    }

    @Config("store.gc-interval")
    public StoreConfig setGarbageCollectionInterval(Duration interval)
    {
        this.garbageCollectionInterval = interval;
        return this;
    }

    @Min(1)
    public int getMaxBatchSize()
    {
        return maxBatchSize;
    }

    @Config("store.remote.max-batch-size")
    public StoreConfig setMaxBatchSize(int maxBatchSize)
    {
        this.maxBatchSize = maxBatchSize;
        return this;
    }

    @Min(1)
    public int getQueueSize()
    {
        return queueSize;
    }

    @Config("store.remote.queue-size")
    public StoreConfig setQueueSize(int queueSize)
    {
        this.queueSize = queueSize;
        return this;
    }

    @MinDuration("1ms")
    @NotNull
    public Duration getRemoteUpdateInterval()
    {
        return remoteUpdateInterval;
    }

    @Config("store.remote.update-interval")
    public StoreConfig setRemoteUpdateInterval(Duration remoteUpdateInterval)
    {
        this.remoteUpdateInterval = remoteUpdateInterval;
        return this;
    }

    @MinDuration("1ms")
    public Duration getReplicationInterval()
    {
        return replicationInterval;
    }

    @Config("store.remote.replication-interval")
    public StoreConfig setReplicationInterval(Duration replicationInterval)
    {
        this.replicationInterval = replicationInterval;
        return this;
    }
}
