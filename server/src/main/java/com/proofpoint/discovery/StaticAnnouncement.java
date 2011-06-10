package com.proofpoint.discovery;

import com.google.common.collect.ImmutableMap;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import javax.validation.constraints.NotNull;
import java.util.Map;

public class StaticAnnouncement
{
    private final String environment;
    private final String location;
    private final String type;
    private final String pool;
    private final Map<String, String> properties;

    @JsonCreator
    public StaticAnnouncement(
            @JsonProperty("environment") String environment,
            @JsonProperty("type") String type,
            @JsonProperty("pool") String pool,
            @JsonProperty("location") String location,
            @JsonProperty("properties") Map<String, String> properties)
    {
        this.environment = environment;
        this.location = location;
        this.type = type;
        this.pool = pool;

        if (properties != null) {
            this.properties = ImmutableMap.copyOf(properties);
        }
        else {
            this.properties = null;
        }
    }

    @NotNull
    public String getEnvironment()
    {
        return environment;
    }

    public String getLocation()
    {
        return location;
    }

    @NotNull
    public String getType()
    {
        return type;
    }

    @NotNull
    public String getPool()
    {
        return pool;
    }

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

        StaticAnnouncement that = (StaticAnnouncement) o;

        if (environment != null ? !environment.equals(that.environment) : that.environment != null) {
            return false;
        }
        if (location != null ? !location.equals(that.location) : that.location != null) {
            return false;
        }
        if (pool != null ? !pool.equals(that.pool) : that.pool != null) {
            return false;
        }
        if (properties != null ? !properties.equals(that.properties) : that.properties != null) {
            return false;
        }
        if (type != null ? !type.equals(that.type) : that.type != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = environment != null ? environment.hashCode() : 0;
        result = 31 * result + (location != null ? location.hashCode() : 0);
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (pool != null ? pool.hashCode() : 0);
        result = 31 * result + (properties != null ? properties.hashCode() : 0);
        return result;
    }
}
