package com.proofpoint.discovery;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Set;

import static com.google.common.base.Predicates.and;
import static com.google.common.collect.Iterables.filter;
import static com.proofpoint.discovery.Service.matchesPool;
import static com.proofpoint.discovery.Service.matchesType;

public class InMemoryStaticStore
    implements StaticStore
{
    private final Map<Id<Service>, Service> services = Maps.newHashMap();

    @Override
    public synchronized void put(Service service)
    {
        Preconditions.checkNotNull(service, "service is null");
        Preconditions.checkArgument(service.getNodeId() == null, "service.nodeId should be null");

        services.put(service.getId(), service);
    }

    @Override
    public synchronized void delete(Id<Service> id)
    {
        services.remove(id);
    }

    @Override
    public synchronized Set<Service> getAll()
    {
        return ImmutableSet.copyOf(services.values());
    }

    @Override
    public synchronized Set<Service> get(String type)
    {
        return ImmutableSet.copyOf(filter(getAll(), matchesType(type)));
    }

    @Override
    public synchronized Set<Service> get(String type, String pool)
    {
        return ImmutableSet.copyOf(filter(getAll(), and(matchesType(type), matchesPool(pool))));
    }
}
