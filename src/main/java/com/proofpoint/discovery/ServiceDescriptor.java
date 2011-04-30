package com.proofpoint.discovery;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;

import javax.annotation.concurrent.Immutable;
import java.util.Map;
import java.util.UUID;

@Immutable
public class ServiceDescriptor
{
    private final UUID id;
    private final UUID nodeId;
    private final String type;
    private final String pool;
    private final String location;
    private final Map<String, String> properties;

    public ServiceDescriptor(UUID id, UUID nodeId, String type, String pool, String location, Map<String, String> properties)
    {
        Preconditions.checkNotNull(id, "id is null");
        Preconditions.checkNotNull(nodeId, "nodeId is null");
        Preconditions.checkNotNull(type, "type is null");
        Preconditions.checkNotNull(pool, "pool is null");
        Preconditions.checkNotNull(location, "location is null");
        Preconditions.checkNotNull(properties, "properties is null");

        this.id = id;
        this.nodeId = nodeId;
        this.type = type;
        this.pool = pool;
        this.location = location;
        this.properties = ImmutableMap.copyOf(properties);
    }

    public UUID getId()
    {
        return id;
    }

    public UUID getNodeId()
    {
        return nodeId;
    }

    public String getType()
    {
        return type;
    }

    public String getPool()
    {
        return pool;
    }

    public String getLocation()
    {
        return location;
    }

    public Map<String, String> getProperties()
    {
        return properties;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ServiceDescriptor that = (ServiceDescriptor) o;

        if (!id.equals(that.id)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return id.hashCode();
    }


    public static Predicate<ServiceDescriptor> matchesType(final String type)
    {
        return new Predicate<ServiceDescriptor>()
        {
            public boolean apply(ServiceDescriptor descriptor)
            {
                return descriptor.getType().equals(type);
            }
        };
    }

    public static Predicate<ServiceDescriptor> matchesPool(final String pool)
    {
        return new Predicate<ServiceDescriptor>()
        {
            public boolean apply(ServiceDescriptor descriptor)
            {
                return descriptor.getPool().equals(pool);
            }
        };
    }

}
