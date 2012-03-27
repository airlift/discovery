package com.proofpoint.discovery.store;

import com.proofpoint.configuration.Config;
import com.proofpoint.units.Duration;

import javax.validation.constraints.NotNull;
import java.util.concurrent.TimeUnit;

public class StoreConfig
{
    private Duration tombstoneMaxAge = new Duration(1, TimeUnit.DAYS);
    private Duration garbageCollectionInterval = new Duration(1, TimeUnit.HOURS);

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
}
