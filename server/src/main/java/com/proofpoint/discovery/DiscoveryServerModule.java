package com.proofpoint.discovery;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.proofpoint.configuration.ConfigurationModule;
import com.proofpoint.discovery.client.ServiceDescriptor;
import com.proofpoint.discovery.client.ServiceInventory;
import com.proofpoint.discovery.client.ServiceSelector;
import com.proofpoint.event.client.EventClient;
import com.proofpoint.event.client.NullEventClient;
import com.proofpoint.node.NodeInfo;

import javax.inject.Singleton;
import java.util.List;

public class DiscoveryServerModule
        implements Module
{
    public void configure(Binder binder)
    {
        binder.bind(DynamicAnnouncementResource.class).in(Scopes.SINGLETON);
        binder.bind(StaticAnnouncementResource.class).in(Scopes.SINGLETON);
        binder.bind(ServiceResource.class).in(Scopes.SINGLETON);

        binder.bind(DynamicStore.class).to(ReplicatedDynamicStore.class).in(Scopes.SINGLETON);
        binder.bind(StaticStore.class).to(ReplicatedStaticStore.class).in(Scopes.SINGLETON);
        
        binder.bind(EventClient.class).to(NullEventClient.class);

        ConfigurationModule.bindConfig(binder).to(DiscoveryConfig.class);
    }

    @Singleton
    @Provides
    public ServiceSelector getServiceInventory(final ServiceInventory inventory, final NodeInfo nodeInfo)
    {
        return new ServiceSelector()
        {
            @Override
            public String getType()
            {
                return "discovery";
            }

            @Override
            public String getPool()
            {
                return nodeInfo.getPool();
            }

            @Override
            public List<ServiceDescriptor> selectAllServices()
            {
                return ImmutableList.copyOf(inventory.getServiceDescriptors(getType()));
            }
        };
    }
}
