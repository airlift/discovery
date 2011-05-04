package com.proofpoint.discovery;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import javax.annotation.concurrent.Immutable;
import javax.validation.constraints.NotNull;
import java.util.Map;
import java.util.UUID;

@Immutable
public class Service
{
    private final UUID id;
    private final UUID nodeId;
    private final String type;
    private final String pool;
    private final String location;
    private final Map<String, String> properties;

    @JsonCreator
    public Service(@JsonProperty("id") UUID id,
                   @JsonProperty("nodeId") UUID nodeId,
                   @JsonProperty("type") String type,
                   @JsonProperty("pool") String pool,
                   @JsonProperty("location") String location,
                   @JsonProperty("properties") Map<String, String> properties)
    {
        this.id = id;
        this.nodeId = nodeId;
        this.type = type;
        this.pool = pool;
        this.location = location;
        this.properties = ImmutableMap.copyOf(properties);
    }

    @JsonProperty
    @NotNull
    public UUID getId()
    {
        return id;
    }

    @JsonProperty
    public UUID getNodeId()
    {
        return nodeId;
    }

    @JsonProperty
    @NotNull
    public String getType()
    {
        return type;
    }

    @JsonProperty
    @NotNull
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
    @NotNull
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

    public static Builder copyOf(Service other)
    {
        return new Builder().copyOf(other);
    }

    public static class Builder
    {
        private UUID id;
        private UUID nodeId;
        private String type;
        private String pool;
        private String location;
        private Map<String, String> properties;

        public Builder copyOf(Service other)
        {
            id = other.id;
            nodeId = other.nodeId;
            type = other.type;
            pool = other.pool;
            location = other.location;
            properties = ImmutableMap.copyOf(other.properties);

            return this;
        }

        public Builder setId(UUID id)
        {
            this.id = id;
            return this;
        }

        public Builder setNodeId(UUID nodeId)
        {
            this.nodeId = nodeId;
            return this;
        }

        public Builder setType(String type)
        {
            this.type = type;
            return this;
        }

        public Builder setPool(String pool)
        {
            this.pool = pool;
            return this;
        }

        public Builder setLocation(String location)
        {
            this.location = location;
            return this;
        }

        public Builder setProperties(Map<String, String> properties)
        {
            this.properties = ImmutableMap.copyOf(properties);
            return this;
        }

        public Service build()
        {
            // TODO: validate state
            return new Service(id, nodeId, type, pool, location, properties);
        }
    }
}
