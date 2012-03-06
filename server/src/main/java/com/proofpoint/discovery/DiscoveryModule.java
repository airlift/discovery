package com.proofpoint.discovery;

import com.google.common.net.InetAddresses;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.proofpoint.cassandra.CassandraServerInfo;
import com.proofpoint.configuration.ConfigurationModule;
import com.proofpoint.node.NodeInfo;
import me.prettyprint.cassandra.service.CassandraHostConfigurator;
import me.prettyprint.cassandra.service.clock.MillisecondsClockResolution;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.factory.HFactory;
import org.joda.time.DateTime;

import static java.lang.String.format;

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
        binder.bind(CassandraStaticStore.class).in(Scopes.SINGLETON);

        binder.bind(DateTime.class).toProvider(RealTimeProvider.class);
        
        binder.bind(Cluster.class).toProvider(ClusterProvider.class);

        ConfigurationModule.bindConfig(binder).to(DiscoveryConfig.class);
        ConfigurationModule.bindConfig(binder).to(CassandraStoreConfig.class);
    }
}
