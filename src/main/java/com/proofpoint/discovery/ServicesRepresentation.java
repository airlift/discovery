package com.proofpoint.discovery;

import org.codehaus.jackson.annotate.JsonProperty;

import java.util.Set;

public class ServicesRepresentation
{
    private final String environment;
    private final Set<ServiceRepresentation> services;

    public ServicesRepresentation(String environment, Set<ServiceRepresentation> services)
    {
        this.environment = environment;
        this.services = services;
    }

    @JsonProperty
    public String getEnvironment()
    {
        return environment;
    }

    @JsonProperty
    public Set<ServiceRepresentation> getServices()
    {
        return services;
    }
}
