package com.proofpoint.discovery;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;
import com.proofpoint.json.JsonCodec;
import org.testng.annotations.Test;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static com.proofpoint.experimental.testing.ValidationAssertions.assertFailsValidation;
import static com.proofpoint.testing.Assertions.assertNotEquals;
import static com.proofpoint.testing.EquivalenceTester.equivalenceTester;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestDynamicServiceAnnouncement
{
    @Test
    public void testValidatesNullId()
    {
        DynamicServiceAnnouncement announcement = new DynamicServiceAnnouncement(null, "type", Collections.<String, String>emptyMap());
        assertFailsValidation(announcement, "id", "may not be null", NotNull.class);
    }

    @Test
    public void testValidatesNullType()
    {
        DynamicServiceAnnouncement announcement = new DynamicServiceAnnouncement(Id.<Service>random(), null, Collections.<String, String>emptyMap());
        assertFailsValidation(announcement, "type", "may not be null", NotNull.class);
    }

    @Test
    public void testValidatesNullProperties()
    {
        DynamicServiceAnnouncement announcement = new DynamicServiceAnnouncement(Id.<Service>random(), "type", null);
        assertFailsValidation(announcement, "properties", "may not be null", NotNull.class);
    }

    @Test
    public void testParsing()
            throws IOException
    {
        JsonCodec<DynamicServiceAnnouncement> codec = JsonCodec.jsonCodec(DynamicServiceAnnouncement.class);

        DynamicServiceAnnouncement parsed = codec.fromJson(Resources.toString(Resources.getResource("dynamic-announcement.json"), Charsets.UTF_8));
        DynamicServiceAnnouncement expected = new DynamicServiceAnnouncement(Id.<Service>valueOf("ff824508-b6a6-4dfc-8f0b-85028465534d"), "blue", ImmutableMap.of("key", "valueA"));

        assertEquals(parsed, expected);
    }

    @Test
    public void testEquivalence()
    {
        equivalenceTester()
                // vary fields, one by one
                .addEquivalentGroup(new DynamicServiceAnnouncement(Id.<Service>valueOf("ff824508-b6a6-4dfc-8f0b-85028465534d"), "blue", ImmutableMap.of("key", "valueA")),
                                    new DynamicServiceAnnouncement(Id.<Service>valueOf("ff824508-b6a6-4dfc-8f0b-85028465534d"), "blue", ImmutableMap.of("key", "valueA")))
                .addEquivalentGroup(new DynamicServiceAnnouncement(Id.<Service>valueOf("ff824508-b6a6-4dfc-8f0b-85028465534d"), "blue", ImmutableMap.of("key", "valueB")),
                                    new DynamicServiceAnnouncement(Id.<Service>valueOf("ff824508-b6a6-4dfc-8f0b-85028465534d"), "blue", ImmutableMap.of("key", "valueB")))
                .addEquivalentGroup(new DynamicServiceAnnouncement(Id.<Service>valueOf("ff824508-b6a6-4dfc-8f0b-85028465534d"), "red", ImmutableMap.of("key", "valueA")),
                                    new DynamicServiceAnnouncement(Id.<Service>valueOf("ff824508-b6a6-4dfc-8f0b-85028465534d"), "red", ImmutableMap.of("key", "valueA")))
                .addEquivalentGroup(new DynamicServiceAnnouncement(Id.<Service>valueOf("4960d071-67b0-4552-8b12-b7abd869aa83"), "blue", ImmutableMap.of("key", "valueA")),
                                    new DynamicServiceAnnouncement(Id.<Service>valueOf("4960d071-67b0-4552-8b12-b7abd869aa83"), "blue", ImmutableMap.of("key", "valueA")))
                // null fields
                .addEquivalentGroup(new DynamicServiceAnnouncement(Id.<Service>valueOf("ff824508-b6a6-4dfc-8f0b-85028465534d"), "blue", null),
                                    new DynamicServiceAnnouncement(Id.<Service>valueOf("ff824508-b6a6-4dfc-8f0b-85028465534d"), "blue", null))
                .addEquivalentGroup(new DynamicServiceAnnouncement(Id.<Service>valueOf("ff824508-b6a6-4dfc-8f0b-85028465534d"), null, ImmutableMap.of("key", "valueA")),
                                    new DynamicServiceAnnouncement(Id.<Service>valueOf("ff824508-b6a6-4dfc-8f0b-85028465534d"), null, ImmutableMap.of("key", "valueA")))
                .addEquivalentGroup(new DynamicServiceAnnouncement(null, "blue", ImmutableMap.of("key", "valueA")),
                                    new DynamicServiceAnnouncement(null, "blue", ImmutableMap.of("key", "valueA")))

                // empty properties
                .addEquivalentGroup(new DynamicServiceAnnouncement(Id.<Service>valueOf("ff824508-b6a6-4dfc-8f0b-85028465534d"), "blue", Collections.<String, String>emptyMap()),
                                    new DynamicServiceAnnouncement(Id.<Service>valueOf("ff824508-b6a6-4dfc-8f0b-85028465534d"), "blue", Collections.<String, String>emptyMap()))
                .check();
    }

    @Test
    public void testCreatesDefensiveCopyOfProperties()
    {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("key", "value");
        DynamicServiceAnnouncement announcement = new DynamicServiceAnnouncement(Id.<Service>random(), "type", properties);

        assertEquals(announcement.getProperties(), properties);
        properties.put("key2", "value2");
        assertNotEquals(announcement.getProperties(), properties);
    }

    @Test
    public void testImmutableProperties()
    {
        DynamicServiceAnnouncement announcement = new DynamicServiceAnnouncement (Id.<Service>random(), "type", ImmutableMap.of("key", "value"));

        try {
            announcement.getProperties().put("key2", "value2");

            // a copy of the internal map is acceptable
            assertEquals(announcement.getProperties(), ImmutableMap.of("key", "value"));
        }
        catch (UnsupportedOperationException e) {
            // an exception is ok, too
        }
    }

    @Test
    public void testToString()
    {
        DynamicServiceAnnouncement announcement = new DynamicServiceAnnouncement(Id.<Service>valueOf("ff824508-b6a6-4dfc-8f0b-85028465534d"), "blue", ImmutableMap.of("key", "valueA"));

        assertNotNull(announcement.toString());
    }
}
