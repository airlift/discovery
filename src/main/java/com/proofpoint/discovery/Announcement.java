package com.proofpoint.discovery;

import com.google.common.collect.ImmutableSet;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import javax.annotation.concurrent.Immutable;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.util.Set;

@Immutable
public class Announcement
{
    private final String environment;
    private final String location;
    private final Set<ServiceAnnouncement> services;

    @JsonCreator
    public Announcement(@JsonProperty("environment") String environment,
                        @JsonProperty("location") String location,
                        @JsonProperty("services") Set<ServiceAnnouncement> services)
    {
        this.environment = environment;
        this.location = location;

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
    @Valid
    public Set<ServiceAnnouncement> getServices()
    {
        return services;
    }
}
