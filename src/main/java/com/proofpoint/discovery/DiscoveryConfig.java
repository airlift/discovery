package com.proofpoint.discovery;

import com.proofpoint.configuration.Config;
import com.proofpoint.units.Duration;

import java.util.concurrent.TimeUnit;

public class DiscoveryConfig
{
    private Duration maxAge = new Duration(30, TimeUnit.SECONDS);

    public Duration getMaxAge()
    {
        return maxAge;
    }

    @Config("discovery.max-age")
    public void setMaxAge(Duration maxAge)
    {
        this.maxAge = maxAge;
    }
}
