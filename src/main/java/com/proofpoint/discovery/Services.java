package com.proofpoint.discovery;

import com.google.common.collect.ImmutableSet;
import org.codehaus.jackson.annotate.JsonProperty;

import javax.annotation.concurrent.Immutable;
import java.util.Set;

@Immutable
public class Services
{
    private final String environment;
    private final Set<Service> services;

    public Services(String environment, Set<Service> services)
    {
        this.environment = environment;
        this.services = ImmutableSet.copyOf(services);
    }

    @JsonProperty
    public String getEnvironment()
    {
        return environment;
    }

    @JsonProperty
    public Set<Service> getServices()
    {
        return services;
    }
}
