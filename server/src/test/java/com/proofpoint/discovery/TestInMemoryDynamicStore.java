package com.proofpoint.discovery;

import org.joda.time.DateTime;

import javax.inject.Provider;

public class TestInMemoryDynamicStore
    extends TestDynamicStore
{
    @Override
    public DynamicStore initializeStore(DiscoveryConfig config, Provider<DateTime> timeProvider)
    {
        return new InMemoryDynamicStore(config, timeProvider);
    }
}
