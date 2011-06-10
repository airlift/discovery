package com.proofpoint.discovery;

import com.google.common.collect.ImmutableMap;
import com.proofpoint.configuration.testing.ConfigAssertions;
import com.proofpoint.units.Duration;
import org.testng.annotations.Test;

import javax.validation.constraints.NotNull;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.proofpoint.experimental.testing.ValidationAssertions.assertFailsValidation;

public class TestDiscoveryConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(DiscoveryConfig.class)
                                                        .setMaxAge(new Duration(30, TimeUnit.SECONDS)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("discovery.max-age", "1m")
                .build();

        DiscoveryConfig expected = new DiscoveryConfig()
                .setMaxAge(new Duration(1, TimeUnit.MINUTES));

        ConfigAssertions.assertFullMapping(properties, expected);
    }


    @Test
    public void testValidatesNotNullDuration()
    {
        DiscoveryConfig config = new DiscoveryConfig().setMaxAge(null);

        assertFailsValidation(config, "maxAge", "may not be null", NotNull.class);
    }
}
