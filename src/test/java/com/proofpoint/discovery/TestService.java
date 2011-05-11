package com.proofpoint.discovery;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;
import com.proofpoint.json.JsonCodec;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import static com.proofpoint.testing.Assertions.assertNotEquals;
import static com.proofpoint.testing.EquivalenceTester.equivalenceTester;
import static org.testng.Assert.assertEquals;

public class TestService
{
    @Test
    public void testEquivalence()
    {
        equivalenceTester()
                .addEquivalentGroup(new Service(UUID.fromString("beb73711-0725-47c3-9305-a69d8c344c1e"), UUID.fromString("3c8285d0-bd0d-4fe2-8321-7ebcd065607b"), "blue", "poolA", "/locationA",
                                                ImmutableMap.of("key", "valueA")),
                                    new Service(UUID.fromString("beb73711-0725-47c3-9305-a69d8c344c1e"), UUID.fromString("3c8285d0-bd0d-4fe2-8321-7ebcd065607b"), "blue", "poolA", "/locationA",
                                                ImmutableMap.of("key", "valueA")))
                .addEquivalentGroup(new Service(UUID.fromString("a0592d5d-b42b-4376-a651-be973ad97750"), UUID.fromString("3c8285d0-bd0d-4fe2-8321-7ebcd065607b"), "blue", "poolA", "/locationA", ImmutableMap.of("key", "valueA")),
                                    new Service(UUID.fromString("a0592d5d-b42b-4376-a651-be973ad97750"), UUID.fromString("3c8285d0-bd0d-4fe2-8321-7ebcd065607b"), "blue", "poolA", "/locationA", ImmutableMap.of("key", "valueA")))
                .check();
    }

    @Test
    public void testCreatesDefensiveCopyOfProperties()
    {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("key", "value");
        Service service = new Service(UUID.randomUUID(), UUID.randomUUID(), "type", "pool", "/location", properties);

        assertEquals(service.getProperties(), properties);
        properties.put("key2", "value2");
        assertNotEquals(service.getProperties(), properties);
    }

    @Test
    public void testImmutableProperties()
    {
        Service service = new Service(UUID.randomUUID(), UUID.randomUUID(), "type", "pool", "/location", ImmutableMap.of("key", "value"));

        try {
            service.getProperties().put("key2", "value2");

            // a copy of the internal map is acceptable
            assertEquals(service.getProperties(), ImmutableMap.of("key", "value"));
        }
        catch (UnsupportedOperationException e) {
            // an exception is ok, too
        }
    }

    @Test
    public void testToJson()
            throws IOException
    {
        JsonCodec<Service> serviceCodec = JsonCodec.jsonCodec(Service.class);
        Service service = new Service(UUID.fromString("c0c5be5f-b298-4cfa-922a-3e5954208444"), UUID.fromString("3ff52f57-04e0-46c3-b606-7497b09dd5c7"), "type", "pool", "/location", ImmutableMap.of("key", "value"));

        String json = serviceCodec.toJson(service);

        JsonCodec<Object> codec = JsonCodec.jsonCodec(Object.class);
        Object parsed = codec.fromJson(json);
        Object expected = codec.fromJson(Resources.toString(Resources.getResource("service.json"), Charsets.UTF_8));

        assertEquals(parsed, expected);
    }

    @Test
    public void testParseJson()
            throws IOException
    {
        JsonCodec<Service> codec = JsonCodec.jsonCodec(Service.class);
        Service parsed = codec.fromJson(Resources.toString(Resources.getResource("service.json"), Charsets.UTF_8));

        Service expected = new Service(UUID.fromString("c0c5be5f-b298-4cfa-922a-3e5954208444"), UUID.fromString("3ff52f57-04e0-46c3-b606-7497b09dd5c7"), "type", "pool", "/location", ImmutableMap.of("key", "value"));

        assertEquals(parsed, expected);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "id.*")
    public void testValidatesIdNotNull()
    {
        new Service(null, UUID.randomUUID(), "type", "pool", "/location", ImmutableMap.of("key", "value"));
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "type.*")
    public void testValidatesTypeNotNull()
    {
        new Service(UUID.randomUUID(), UUID.randomUUID(), null, "pool", "/location", ImmutableMap.of("key", "value"));
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "pool.*")
    public void testValidatesPoolNotNull()
    {
        new Service(UUID.randomUUID(), UUID.randomUUID(), "type", null, "/location", ImmutableMap.of("key", "value"));
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "location.*")
    public void testValidatesLocationNotNull()
    {
        new Service(UUID.randomUUID(), UUID.randomUUID(), "type", "type", null, ImmutableMap.of("key", "value"));
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "properties.*")
    public void testValidatesPropertiesNotNull()
    {
        new Service(UUID.randomUUID(), UUID.randomUUID(), "type", "type", "/location", null);
    }
}
