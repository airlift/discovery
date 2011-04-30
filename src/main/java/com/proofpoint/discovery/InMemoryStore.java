package com.proofpoint.discovery;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.MapMaker;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Predicates.and;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.filter;
import static com.proofpoint.discovery.ServiceDescriptor.matchesPool;
import static com.proofpoint.discovery.ServiceDescriptor.matchesType;

@ThreadSafe
public class InMemoryStore
        implements Store
{
    private final ConcurrentMap<UUID, Set<ServiceDescriptor>> descriptors;

    @Inject
    public InMemoryStore(DiscoveryConfig config)
    {
        descriptors = new MapMaker().expireAfterWrite((long) config.getMaxAge().toMillis(), TimeUnit.MILLISECONDS).makeMap();
    }

    @Override
    public void put(UUID nodeId, Set<ServiceDescriptor> descriptors)
    {
        Preconditions.checkNotNull(nodeId, "nodeId is null");
        Preconditions.checkNotNull(descriptors, "descriptors is null");

        this.descriptors.put(nodeId, ImmutableSet.copyOf(descriptors));
    }

    @Override
    public Set<ServiceDescriptor> delete(UUID nodeId)
    {
        Preconditions.checkNotNull(nodeId, "nodeId is null");

        return descriptors.remove(nodeId);
    }

    @Override
    public Set<ServiceDescriptor> get(String type)
    {
        Preconditions.checkNotNull(type, "type is null");

        return ImmutableSet.copyOf(filter(concat(descriptors.values()), matchesType(type)));
    }

    @Override
    public Set<ServiceDescriptor> get(String type, String pool)
    {
        Preconditions.checkNotNull(type, "type is null");
        Preconditions.checkNotNull(pool, "pool is null");

        return ImmutableSet.copyOf(filter(concat(descriptors.values()),
                                          and(matchesType(type), matchesPool(pool))));
    }
}
