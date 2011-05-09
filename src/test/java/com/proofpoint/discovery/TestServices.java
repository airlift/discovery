package com.proofpoint.discovery;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.io.Resources;
import com.proofpoint.experimental.json.JsonCodec;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;

import static com.proofpoint.experimental.json.JsonCodec.jsonCodec;
import static com.proofpoint.testing.Assertions.assertNotEquals;
import static org.testng.Assert.assertEquals;

public class TestServices
{
    @Test
    public void testCreatesDefensiveCopyOfServices()
    {
        Set<Service> set = Sets.newHashSet();
        set.add(new Service(UUID.randomUUID(), UUID.randomUUID(), "blue", "pool", "/location", ImmutableMap.of("key", "value")));

        Services services = new Services("testing", set);
        assertEquals(services.getServices(), set);

        set.add(new Service(UUID.randomUUID(), UUID.randomUUID(), "red", "pool", "/location", ImmutableMap.of("key", "value")));
        assertNotEquals(services.getServices(), set);
    }

    @Test
    public void testServicesSetIsImmutable()
    {
        Service blue = new Service(UUID.randomUUID(), UUID.randomUUID(), "blue", "pool", "/location", ImmutableMap.of("key", "value"));

        Services services = new Services("testing", Sets.newHashSet(blue));
        try {
            services.getServices().add(new Service(UUID.randomUUID(), UUID.randomUUID(), "red", "pool", "/location", ImmutableMap.of("key", "value")));

            // a copy of the internal map is acceptable
            assertEquals(services.getServices(), ImmutableSet.of(blue));
        }
        catch (UnsupportedOperationException e) {
            // an exception is ok, too
        }
    }

    @Test
    public void testToJson()
            throws IOException
    {
        Service blue = new Service(UUID.fromString("c0c5be5f-b298-4cfa-922a-3e5954208444"), UUID.fromString("3ff52f57-04e0-46c3-b606-7497b09dd5c7"), "blue", "poolA", "/locationA", ImmutableMap.of("key", "valueA"));
        Service red = new Service(UUID.fromString("e3780aba-98fe-4de1-b682-5cd7a3264367"), UUID.fromString("989c4a90-ef68-4a05-8ad9-73a02aea406c"), "red", "poolB", "/locationB", ImmutableMap.of("key", "valueB"));
        Services services = new Services("testing", ImmutableSet.of(blue, red));

        String json = jsonCodec(Services.class).toJson(services);

        JsonCodec<Object> codec = jsonCodec(Object.class);
        Object parsed = codec.fromJson(json);
        Object expected = codec.fromJson(Resources.toString(Resources.getResource("services.json"), Charsets.UTF_8));

        assertEquals(parsed, expected);
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "environment.*")
    public void testValidatesEnvironmentNotNull()
    {
        new Services(null, Collections.<Service>emptySet());
    }

    @Test(expectedExceptions = NullPointerException.class, expectedExceptionsMessageRegExp = "services.*")
    public void testValidatesServicesNotNull()
    {
        new Services("testing", null);
    }
}
