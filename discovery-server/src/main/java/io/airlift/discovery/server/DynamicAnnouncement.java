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
import com.google.common.collect.ImmutableSet;

import com.google.errorprone.annotations.Immutable;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

import java.util.Objects;
import java.util.Set;

@Immutable
public class DynamicAnnouncement
{
    private final String environment;
    private final String location;
    private final String pool;
    private final Set<DynamicServiceAnnouncement> services;

    @JsonCreator
    public DynamicAnnouncement(
            @JsonProperty("environment") String environment,
            @JsonProperty("pool") String pool,
            @JsonProperty("location") String location,
            @JsonProperty("services") Set<DynamicServiceAnnouncement> services)
    {
        this.environment = environment;
        this.location = location;
        this.pool = pool;

        if (services != null) {
            this.services = ImmutableSet.copyOf(services);
        }
        else {
            this.services = null;
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
    public String getPool()
    {
        return pool;
    }

    @NotNull
    @Valid
    public Set<DynamicServiceAnnouncement> getServiceAnnouncements()
    {
        return services;
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

        DynamicAnnouncement that = (DynamicAnnouncement) o;

        if (!Objects.equals(environment, that.environment)) {
            return false;
        }
        if (!Objects.equals(location, that.location)) {
            return false;
        }
        if (!Objects.equals(pool, that.pool)) {
            return false;
        }
        return Objects.equals(services, that.services);
    }

    @Override
    public int hashCode()
    {
        int result = environment != null ? environment.hashCode() : 0;
        result = 31 * result + (location != null ? location.hashCode() : 0);
        result = 31 * result + (pool != null ? pool.hashCode() : 0);
        result = 31 * result + (services != null ? services.hashCode() : 0);
        return result;
    }

    @Override
    public String toString()
    {
        return "DynamicAnnouncement{" +
                "environment='" + environment + '\'' +
                ", location='" + location + '\'' +
                ", pool='" + pool + '\'' +
                ", services=" + services +
                '}';
    }

    public static Builder copyOf(DynamicAnnouncement announcement)
    {
        return new Builder().copyOf(announcement);
    }

    public static class Builder
    {
        private String environment;
        private String location;
        private String pool;
        private Set<DynamicServiceAnnouncement> services;

        public Builder copyOf(DynamicAnnouncement announcement)
        {
            environment = announcement.getEnvironment();
            location = announcement.getLocation();
            services = announcement.getServiceAnnouncements();
            pool = announcement.getPool();

            return this;
        }

        public Builder setLocation(String location)
        {
            this.location = location;
            return this;
        }

        public DynamicAnnouncement build()
        {
            return new DynamicAnnouncement(environment, pool, location, services);
        }
    }
}
