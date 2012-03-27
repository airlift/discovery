package com.proofpoint.discovery.store;

import com.proofpoint.configuration.Config;
import com.proofpoint.units.Duration;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.concurrent.TimeUnit;

public class StoreConfig
{
    private Duration tombstoneMaxAge = new Duration(1, TimeUnit.DAYS);
    private Duration garbageCollectionInterval = new Duration(1, TimeUnit.HOURS);
    private int maxBatchSize = 1000;
    private int queueSize = 1000;

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
}
