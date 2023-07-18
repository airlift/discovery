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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;

import jakarta.validation.constraints.NotNull;

import java.util.Map;
import java.util.Objects;

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

        if (!Objects.equals(environment, that.environment)) {
            return false;
        }
        if (!Objects.equals(location, that.location)) {
            return false;
        }
        if (!Objects.equals(pool, that.pool)) {
            return false;
        }
        if (!Objects.equals(properties, that.properties)) {
            return false;
        }
        return Objects.equals(type, that.type);
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
