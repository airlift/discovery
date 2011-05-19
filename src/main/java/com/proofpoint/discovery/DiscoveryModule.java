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
        binder.bind(DynamicAnnouncementResource.class).in(Scopes.SINGLETON);
        binder.bind(StaticAnnouncementResource.class).in(Scopes.SINGLETON);
        binder.bind(ServiceResource.class).in(Scopes.SINGLETON);

        binder.bind(DynamicStore.class).to(CassandraDynamicStore.class).in(Scopes.SINGLETON);
        binder.bind(CassandraDynamicStore.class).in(Scopes.SINGLETON);

        binder.bind(StaticStore.class).to(CassandraStaticStore.class).in(Scopes.SINGLETON);
        binder.bind(DateTime.class).toProvider(RealTimeProvider.class);

        ConfigurationModule.bindConfig(binder).to(DiscoveryConfig.class);
        ConfigurationModule.bindConfig(binder).to(CassandraStoreConfig.class);
    }
}
