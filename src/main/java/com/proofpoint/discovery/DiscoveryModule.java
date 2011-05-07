package com.proofpoint.discovery;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.proofpoint.configuration.ConfigurationModule;
import org.joda.time.DateTime;

public class DiscoveryModule
        implements Module
{
    public void configure(Binder binder)
    {
        binder.bind(AnnouncementResource.class).in(Scopes.SINGLETON);
        binder.bind(ServiceResource.class).in(Scopes.SINGLETON);
        binder.bind(Store.class).to(CassandraStore.class).in(Scopes.SINGLETON);
        binder.bind(DateTime.class).toProvider(RealTimeProvider.class).in(Scopes.SINGLETON);

        ConfigurationModule.bindConfig(binder).to(DiscoveryConfig.class);
        ConfigurationModule.bindConfig(binder).to(CassandraStoreConfig.class);
    }
}
