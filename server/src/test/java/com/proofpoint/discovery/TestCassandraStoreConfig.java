package com.proofpoint.discovery;

import com.google.common.collect.ImmutableMap;
import com.proofpoint.configuration.testing.ConfigAssertions;
import org.testng.annotations.Test;

import javax.validation.constraints.NotNull;
import java.util.Map;

import static com.proofpoint.experimental.testing.ValidationAssertions.assertFailsValidation;

public class TestCassandraStoreConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(CassandraStoreConfig.class)
                .setStaticKeyspace("announcements")
                .setDynamicKeyspace("dynamic_announcements")
                .setReplicationFactor(3)
        );
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("static-store.keyspace", "staticKeyspace")
                .put("dynamic-store.keyspace", "dynamicKeyspace")
                .put("store.replication-factor", "1")
                .build();

        CassandraStoreConfig expected = new CassandraStoreConfig()
                .setStaticKeyspace("staticKeyspace")
                .setDynamicKeyspace("dynamicKeyspace")
                .setReplicationFactor(1);

        ConfigAssertions.assertFullMapping(properties, expected);
    }

    @Test
    public void testValidatesNotNullKeyspace()
    {
        CassandraStoreConfig config = new CassandraStoreConfig()
                .setStaticKeyspace(null)
                .setDynamicKeyspace(null);

        assertFailsValidation(config, "staticKeyspace", "may not be null", NotNull.class);
        assertFailsValidation(config, "dynamicKeyspace", "may not be null", NotNull.class);
    }
}
