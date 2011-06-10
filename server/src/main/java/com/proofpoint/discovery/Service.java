package com.proofpoint.discovery;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import javax.annotation.concurrent.Immutable;
import java.util.Map;

@Immutable
public class Service
{
    private final Id<Service> id;
    private final Id<Node> nodeId;
    private final String type;
    private final String pool;
    private final String location;
    private final Map<String, String> properties;

    @JsonCreator
    public Service(
            @JsonProperty("id") Id<Service> id,
            @JsonProperty("nodeId") Id<Node> nodeId,
            @JsonProperty("type") String type,
            @JsonProperty("pool") String pool,
            @JsonProperty("location") String location,
            @JsonProperty("properties") Map<String, String> properties)
    {
        Preconditions.checkNotNull(id, "id is null");
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

    @JsonProperty
    public Id<Service> getId()
    {
        return id;
    }

    @JsonProperty
    public Id<Node> getNodeId()
    {
        return nodeId;
    }

    @JsonProperty
    public String getType()
    {
        return type;
    }

    @JsonProperty
    public String getPool()
    {
        return pool;
    }

    @JsonProperty
    public String getLocation()
    {
        return location;
    }

    @JsonProperty
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

        Service that = (Service) o;

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


    public static Predicate<Service> matchesType(final String type)
    {
        return new Predicate<Service>()
        {
            public boolean apply(Service descriptor)
            {
                return descriptor.getType().equals(type);
            }
        };
    }

    public static Predicate<Service> matchesPool(final String pool)
    {
        return new Predicate<Service>()
        {
            public boolean apply(Service descriptor)
            {
                return descriptor.getPool().equals(pool);
            }
        };
    }

    @Override
    public String toString()
    {
        return "Service{" +
                "id=" + id +
                ", nodeId=" + nodeId +
                ", type='" + type + '\'' +
                ", pool='" + pool + '\'' +
                ", location='" + location + '\'' +
                ", properties=" + properties +
                '}';
    }

    public static Builder copyOf(StaticAnnouncement announcement)
    {
        return new Builder().copyOf(announcement);
    }

    public static class Builder
    {
        private Id<Service> id;
        private String type;
        private String pool;
        private String location;
        private Map<String, String> properties;

        public Builder copyOf(StaticAnnouncement announcement)
        {
            type = announcement.getType();
            pool = announcement.getPool();
            properties = ImmutableMap.copyOf(announcement.getProperties());

            return this;
        }

        public Builder setId(Id<Service> id)
        {
            this.id = id;
            return this;
        }

        public Builder setLocation(String location)
        {
            this.location = location;
            return this;
        }

        public Service build()
        {
            // TODO: validate state
            return new Service(id, null, type, pool, location, properties);
        }
    }
}
