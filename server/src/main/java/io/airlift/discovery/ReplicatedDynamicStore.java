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
package io.airlift.discovery;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableSet;
import io.airlift.discovery.store.DistributedStore;
import io.airlift.discovery.store.Entry;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;

import javax.inject.Inject;

import java.util.List;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Predicates.and;
import static com.google.common.collect.Iterables.filter;
import static io.airlift.discovery.DynamicServiceAnnouncement.toServiceWith;
import static io.airlift.discovery.Service.matchesPool;
import static io.airlift.discovery.Service.matchesType;

public class ReplicatedDynamicStore
        implements DynamicStore
{
    private final DistributedStore store;
    private final Duration maxAge;
    private final JsonCodec<List<Service>> codec;

    @Inject
    public ReplicatedDynamicStore(@ForDynamicStore DistributedStore store, DiscoveryConfig config, JsonCodec<List<Service>> codec)
    {
        this.store = checkNotNull(store, "store is null");
        this.maxAge = checkNotNull(config, "config is null").getMaxAge();
        this.codec = checkNotNull(codec, "codec is null");
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
        ImmutableSet.Builder<Service> builder = ImmutableSet.builder();
        for (Entry entry : store.getAll()) {
            builder.addAll(codec.fromJson(entry.getValue()));
        }

        return builder.build();
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
}
