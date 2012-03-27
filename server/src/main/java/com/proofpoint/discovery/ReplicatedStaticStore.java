package com.proofpoint.discovery;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

public class ReplicatedStaticStore
    implements StaticStore
{
    @Override
    public void put(Service service)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void delete(Id<Service> nodeId)
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
