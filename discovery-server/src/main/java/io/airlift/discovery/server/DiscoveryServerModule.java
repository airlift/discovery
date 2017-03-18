/*
 * Copyright 2010 Proofpoint, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.airlift.discovery.server;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceInventory;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.discovery.store.InMemoryStore;
import io.airlift.discovery.store.PersistentStore;
import io.airlift.discovery.store.PersistentStoreConfig;
import io.airlift.discovery.store.ReplicatedStoreModule;
import io.airlift.node.NodeInfo;

import javax.inject.Singleton;

import java.util.List;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.discovery.client.DiscoveryBinder.discoveryBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;

public class DiscoveryServerModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(DiscoveryConfig.class);
        jaxrsBinder(binder).bind(ServiceResource.class);

        discoveryBinder(binder).bindHttpAnnouncement("discovery");

        jsonCodecBinder(binder).bindJsonCodec(Service.class);
        jsonCodecBinder(binder).bindListJsonCodec(Service.class);

        // dynamic announcements
        jaxrsBinder(binder).bind(DynamicAnnouncementResource.class);
        binder.bind(DynamicStore.class).to(ReplicatedDynamicStore.class).in(Scopes.SINGLETON);
        binder.install(new ReplicatedStoreModule("dynamic", ForDynamicStore.class, InMemoryStore.class));

        // static announcements
        jaxrsBinder(binder).bind(StaticAnnouncementResource.class);
        binder.bind(StaticStore.class).to(ReplicatedStaticStore.class).in(Scopes.SINGLETON);
        binder.install(new ReplicatedStoreModule("static", ForStaticStore.class, PersistentStore.class));
        configBinder(binder).bindConfig(PersistentStoreConfig.class, "static");
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

            @Override
            public ListenableFuture<List<ServiceDescriptor>> refresh()
            {
                // this should be async, but it is never used
                inventory.updateServiceInventory();
                return Futures.immediateFuture(selectAllServices());
            }
        };
    }
}
