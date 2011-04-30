package com.proofpoint.discovery;

import com.google.common.collect.ImmutableMap;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import javax.annotation.concurrent.Immutable;
import javax.validation.constraints.NotNull;
import java.util.Map;
import java.util.UUID;

@Immutable
public class ServiceRepresentation
{
    private final UUID id;
    private final UUID nodeId;
    private final String type;
    private final String pool;
    private final String location;
    private final Map<String, String> properties;

    @JsonCreator
    public ServiceRepresentation(@JsonProperty("id") UUID id,
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
}
