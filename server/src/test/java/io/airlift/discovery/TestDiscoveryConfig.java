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

import com.google.common.collect.ImmutableMap;
import io.airlift.configuration.testing.ConfigAssertions;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import javax.validation.constraints.NotNull;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.airlift.testing.ValidationAssertions.assertFailsValidation;

public class TestDiscoveryConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(DiscoveryConfig.class)
                .setMaxAge(new Duration(30, TimeUnit.SECONDS))
                .setProxyTypes(DiscoveryConfig.StringSet.of()));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("discovery.max-age", "1m")
                .put("discovery.proxy.types", "foo  ,  bar")
                .build();

        DiscoveryConfig expected = new DiscoveryConfig()
                .setMaxAge(new Duration(1, TimeUnit.MINUTES))
                .setProxyTypes(DiscoveryConfig.StringSet.of("foo", "bar"));

        ConfigAssertions.assertFullMapping(properties, expected);
    }

    @Test
    public void testDeprecatedProperties()
    {
        ConfigAssertions.assertDeprecatedEquivalence(DiscoveryConfig.class,
                ImmutableMap.<String, String>of());
    }

    @Test
    public void testValidatesNotNullDuration()
    {
        DiscoveryConfig config = new DiscoveryConfig().setMaxAge(null);

        assertFailsValidation(config, "maxAge", "may not be null", NotNull.class);
    }
}
