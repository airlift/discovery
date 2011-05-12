package com.proofpoint.discovery;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;
import com.proofpoint.json.JsonCodec;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Map;

import static com.proofpoint.testing.Assertions.assertNotEquals;
import static com.proofpoint.testing.EquivalenceTester.equivalenceTester;
import static org.testng.Assert.assertEquals;

public class TestService
{
    @Test
    public void testEquivalence()
    {
        equivalenceTester()
                .addEquivalentGroup(new Service(Id.<Service>valueOf("beb73711-0725-47c3-9305-a69d8c344c1e"), Id.<Node>valueOf("3c8285d0-bd0d-4fe2-8321-7ebcd065607b"), "blue", "poolA", "/locationA",
                                                ImmutableMap.of("key", "valueA")),
                                    new Service(Id.<Service>valueOf("beb73711-0725-47c3-9305-a69d8c344c1e"), Id.<Node>valueOf("3c8285d0-bd0d-4fe2-8321-7ebcd065607b"), "blue", "poolA", "/locationA",
                                                ImmutableMap.of("key", "valueA")))
                .addEquivalentGroup(new Service(Id.<Service>valueOf("a0592d5d-b42b-4376-a651-be973ad97750"), Id.<Node>valueOf("3c8285d0-bd0d-4fe2-8321-7ebcd065607b"), "blue", "poolA", "/locationA",
                                                ImmutableMap.of("key", "valueA")),
                                    new Service(Id.<Service>valueOf("a0592d5d-b42b-4376-a651-be973ad97750"), Id.<Node>valueOf("3c8285d0-bd0d-4fe2-8321-7ebcd065607b"), "blue", "poolA", "/locationA",
                                                ImmutableMap.of("key", "valueA")))
                .check();
    }

    @Test
    public void testCreatesDefensiveCopyOfProperties()
    {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("key", "value");
        Service service = new Service(Id.<Service>random(), Id.<Node>random(), "type", "pool", "/location", properties);

        assertEquals(service.getProperties(), properties);
        properties.put("key2", "value2");
        assertNotEquals(service.getProperties(), properties);
    }

    @Test
    public void testImmutableProperties()
    {
        Service service = new Service(Id.<Service>random(), Id.<Node>random(), "type", "pool", "/location", ImmutableMap.of("key", "value"));

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
        Service service = new Service(Id.<Service>valueOf("c0c5be5f-b298-4cfa-922a-3e5954208444"), Id.<Node>valueOf("3ff52f57-04e0-46c3-b606-7497b09dd5c7"), "type", "pool", "/location", ImmutableMap.of("key", "value"));

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

        Service expected = new Service(Id.<Service>valueOf("c0c5be5f-b298-4cfa-922a-3e5954208444"), Id.<Node>valueOf("3ff52f57-04e0-46c3-b606-7497b09dd5c7"), "type", "pool", "/location", ImmutableMap.of("key", "value"));

        assertEquals(parsed, expected);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "id.*")
    public void testValidatesIdNotNull()
    {
        new Service(null, Id.<Node>random(), "type", "pool", "/location", ImmutableMap.of("key", "value"));
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "type.*")
    public void testValidatesTypeNotNull()
    {
        new Service(Id.<Service>random(), Id.<Node>random(), null, "pool", "/location", ImmutableMap.of("key", "value"));
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "pool.*")
    public void testValidatesPoolNotNull()
    {
        new Service(Id.<Service>random(), Id.<Node>random(), "type", null, "/location", ImmutableMap.of("key", "value"));
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "location.*")
    public void testValidatesLocationNotNull()
    {
        new Service(Id.<Service>random(), Id.<Node>random(), "type", "type", null, ImmutableMap.of("key", "value"));
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "properties.*")
    public void testValidatesPropertiesNotNull()
    {
        new Service(Id.<Service>random(), Id.<Node>random(), "type", "type", "/location", null);
    }
}
