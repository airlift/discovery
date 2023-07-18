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

import io.airlift.configuration.Config;
import io.airlift.units.Duration;

import jakarta.validation.constraints.NotNull;

import java.util.concurrent.TimeUnit;

public class DiscoveryConfig
{
    private Duration maxAge = new Duration(30, TimeUnit.SECONDS);
    private Duration storeCacheTtl = new Duration(1, TimeUnit.SECONDS);

    @NotNull
    public Duration getMaxAge()
    {
        return maxAge;
    }

    @Config("discovery.max-age")
    public DiscoveryConfig setMaxAge(Duration maxAge)
    {
        this.maxAge = maxAge;
        return this;
    }

    @NotNull
    public Duration getStoreCacheTtl()
    {
        return storeCacheTtl;
    }

    @Config("discovery.store-cache-ttl")
    public DiscoveryConfig setStoreCacheTtl(Duration storeCacheTtl)
    {
        this.storeCacheTtl = storeCacheTtl;
        return this;
    }
}
