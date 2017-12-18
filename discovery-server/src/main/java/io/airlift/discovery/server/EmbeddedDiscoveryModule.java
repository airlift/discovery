/*
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

import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Scopes;
import io.airlift.configuration.AbstractConfigurationAwareModule;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.discovery.store.InMemoryStore;
import io.airlift.discovery.store.ReplicatedStoreModule;

import java.util.Set;

import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.discovery.client.DiscoveryBinder.discoveryBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static io.airlift.json.JsonCodecBinder.jsonCodecBinder;

public class EmbeddedDiscoveryModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        configBinder(binder).bindConfig(DiscoveryConfig.class);
        jaxrsBinder(binder).bind(ServiceResource.class);

        discoveryBinder(binder).bindHttpAnnouncement("discovery");

        jsonCodecBinder(binder).bindJsonCodec(Service.class);
        jsonCodecBinder(binder).bindListJsonCodec(Service.class);

        binder.bind(ServiceSelector.class).to(DiscoveryServiceSelector.class);
        binder.bind(StaticStore.class).to(EmptyStaticStore.class);

        jaxrsBinder(binder).bind(DynamicAnnouncementResource.class);
        binder.bind(DynamicStore.class).to(ReplicatedDynamicStore.class).in(Scopes.SINGLETON);
        binder.install(new ReplicatedStoreModule("dynamic", ForDynamicStore.class, InMemoryStore.class));
    }

    private static class EmptyStaticStore
            implements StaticStore
    {
        @Override
        public void put(Service service)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void delete(Id<Service> id)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Set<Service> getAll()
        {
            return ImmutableSet.of();
        }

        @Override
        public Set<Service> get(String type)
        {
            return ImmutableSet.of();
        }

        @Override
        public Set<Service> get(String type, String pool)
        {
            return ImmutableSet.of();
        }
    }
}
