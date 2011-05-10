package com.proofpoint.discovery;

import com.google.inject.Binder;
import com.google.inject.Module;

public class TestingCassandraModule
    implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(CassandraServerInfo.class).toInstance(CassandraServerSetup.getServerInfo());
    }
}
