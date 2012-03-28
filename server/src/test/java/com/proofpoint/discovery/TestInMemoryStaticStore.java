package com.proofpoint.discovery;

import org.joda.time.DateTime;

import javax.inject.Provider;

public class TestInMemoryStaticStore
    extends TestStaticStore
{
    @Override
    protected StaticStore initializeStore(Provider<DateTime> timeProvider)
    {
        return new InMemoryStaticStore();
    }
}
