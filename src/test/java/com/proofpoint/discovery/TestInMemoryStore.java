package com.proofpoint.discovery;

import org.joda.time.DateTime;

import javax.inject.Provider;

public class TestInMemoryStore
    extends TestStore
{
    @Override
    public Store initializeStore(DiscoveryConfig config, Provider<DateTime> timeProvider)
    {
        return new InMemoryStore(config, timeProvider);
    }
}
