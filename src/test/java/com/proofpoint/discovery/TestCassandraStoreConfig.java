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
                                                        .setKeyspace("announcements"));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("store.cassandra.keyspace", "keyspace")
                .build();

        CassandraStoreConfig expected = new CassandraStoreConfig()
                .setKeyspace("keyspace");

        ConfigAssertions.assertFullMapping(properties, expected);
    }

    @Test
    public void testValidatesNotNullKeyspace()
    {
        CassandraStoreConfig config = new CassandraStoreConfig().setKeyspace(null);

        assertFailsValidation(config, "keyspace", "may not be null", NotNull.class);
    }
}
