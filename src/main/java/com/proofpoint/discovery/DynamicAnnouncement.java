package com.proofpoint.discovery;

import com.google.common.collect.ImmutableSet;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import javax.annotation.concurrent.Immutable;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
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

        if (environment != null ? !environment.equals(that.environment) : that.environment != null) {
            return false;
        }
        if (location != null ? !location.equals(that.location) : that.location != null) {
            return false;
        }
        if (pool != null ? !pool.equals(that.pool) : that.pool != null) {
            return false;
        }
        if (services != null ? !services.equals(that.services) : that.services != null) {
            return false;
        }

        return true;
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
