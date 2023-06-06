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

import com.google.common.base.Supplier;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.discovery.store.DistributedStore;
import io.airlift.discovery.store.Entry;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.and;
import static com.google.common.base.Suppliers.memoizeWithExpiration;
import static com.google.common.collect.Iterables.filter;
import static io.airlift.discovery.server.DynamicServiceAnnouncement.toServiceWith;
import static io.airlift.discovery.server.Service.matchesPool;
import static io.airlift.discovery.server.Service.matchesType;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class ReplicatedDynamicStore
        implements DynamicStore
{
    private final DistributedStore store;
    private final Duration maxAge;
    private final JsonCodec<List<Service>> codec;
    private final Supplier<Set<Service>> servicesSupplier;

    @Inject
    public ReplicatedDynamicStore(@ForDynamicStore DistributedStore store, DiscoveryConfig config, JsonCodec<List<Service>> codec)
    {
        this.store = checkNotNull(store, "store is null");
        this.maxAge = checkNotNull(config, "config is null").getMaxAge();
        this.codec = checkNotNull(codec, "codec is null");

        servicesSupplier = cachingSupplier(servicesSupplier(), config.getStoreCacheTtl());
    }

    @Override
    public void put(Id<Node> nodeId, DynamicAnnouncement announcement)
    {
        List<Service> services = FluentIterable.from(announcement.getServiceAnnouncements())
                .transform(toServiceWith(nodeId, announcement.getLocation(), announcement.getPool()))
                .toList();

        byte[] key = nodeId.getBytes();
        byte[] value = codec.toJsonBytes(services);

        store.put(key, value, maxAge);
    }

    @Override
    public void delete(Id<Node> nodeId)
    {
        store.delete(nodeId.getBytes());
    }

    @Override
    public Set<Service> getAll()
    {
        return servicesSupplier.get();
    }

    @Override
    public Set<Service> get(String type)
    {
        return ImmutableSet.copyOf(filter(getAll(), matchesType(type)));
    }

    @Override
    public Set<Service> get(String type, String pool)
    {
        return ImmutableSet.copyOf(filter(getAll(), and(matchesType(type), matchesPool(pool))));
    }

    private Supplier<Set<Service>> servicesSupplier()
    {
        return new Supplier<Set<Service>>()
        {
            @Override
            public Set<Service> get()
            {
                ImmutableSet.Builder<Service> builder = ImmutableSet.builder();
                for (Entry entry : store.getAll()) {
                    builder.addAll(codec.fromJson(entry.getValue()));
                }
                return builder.build();
            }
        };
    }

    private static <T> Supplier<T> cachingSupplier(Supplier<T> supplier, Duration ttl)
    {
        if (ttl.toMillis() == 0) {
            return supplier;
        }
        return memoizeWithExpiration(supplier, ttl.toMillis(), MILLISECONDS);
    }
}
