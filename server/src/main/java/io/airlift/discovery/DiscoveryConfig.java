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
package io.airlift.discovery;

import com.google.common.collect.ForwardingSet;
import com.google.common.collect.ImmutableSet;
import io.airlift.configuration.Config;
import io.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;

import javax.validation.constraints.AssertTrue;
import javax.validation.constraints.NotNull;
import java.net.URI;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class DiscoveryConfig
{
    private Duration maxAge = new Duration(30, TimeUnit.SECONDS);
    private StringSet proxyProxiedTypes = StringSet.of();
    private String proxyEnvironment = null;
    private URI proxyUri = null;

    @NotNull
    public Duration getMaxAge()
    {
        return maxAge;
    }

    @Config("discovery.max-age")
    @ConfigDescription("Dynamic announcement expiration")
    public DiscoveryConfig setMaxAge(Duration maxAge)
    {
        this.maxAge = maxAge;
        return this;
    }

    public StringSet getProxyProxiedTypes()
    {
        return proxyProxiedTypes;
    }

    @Config("discovery.proxy.proxied-types")
    @ConfigDescription("Service types to proxy (test environments only)")
    public DiscoveryConfig setProxyProxiedTypes(StringSet proxyProxiedTypes)
    {
        this.proxyProxiedTypes = proxyProxiedTypes;
        return this;
    }

    public String getProxyEnvironment()
    {
        return proxyEnvironment;
    }

    @Config("discovery.proxy.environment")
    @ConfigDescription("Environment to proxy to (test environments only)")
    public DiscoveryConfig setProxyEnvironment(String proxyEnvironment)
    {
        this.proxyEnvironment = proxyEnvironment;
        return this;
    }

    public URI getProxyUri()
    {
        return proxyUri;
    }

    @Config("discovery.proxy.uri")
    @ConfigDescription("Discovery server to proxy to (test environments only)")
    public DiscoveryConfig setProxyUri(URI proxyUri)
    {
        this.proxyUri = proxyUri;
        return this;
    }

    @AssertTrue(message = "discovery.proxy.environment specified if and only if any proxy types")
    public boolean isProxyTypeAndEnvironment()
    {
        return proxyProxiedTypes.isEmpty() == (proxyEnvironment == null);
    }

    @AssertTrue(message = "discovery.proxy.uri specified if and only if any proxy types")
    public boolean isProxyTypeAndUri()
    {
        return proxyProxiedTypes.isEmpty() == (proxyUri == null);
    }

    public static final class StringSet extends ForwardingSet<String>
    {
        private final Set<String> delegate;

        private StringSet(Set<String> delegate)
        {
            this.delegate = ImmutableSet.copyOf(delegate);
        }

        public static StringSet of(String... strings)
        {
            return new StringSet(ImmutableSet.copyOf(strings));
        }

        public static StringSet valueOf(String string)
        {
            return of(string.split("\\s*,\\s*"));
        }

        @Override
        protected Set<String> delegate()
        {
            return delegate;
        }
    }
}
