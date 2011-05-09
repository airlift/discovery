package com.proofpoint.discovery;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;
import com.proofpoint.experimental.json.JsonCodec;
import org.testng.annotations.Test;

import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.proofpoint.testing.Assertions.assertInstanceOf;
import static com.proofpoint.testing.Assertions.assertNotEquals;
import static com.proofpoint.testing.EquivalenceTester.equivalenceTester;
import static org.testng.Assert.assertEquals;

public class TestDynamicServiceAnnouncement
{
    private static final Validator VALIDATOR = Validation.buildDefaultValidatorFactory().getValidator();

    @Test
    public void testValidatesNullId()
    {
        DynamicServiceAnnouncement announcement = new DynamicServiceAnnouncement(null, "type", "pool", Collections.<String, String>emptyMap());
        assertFailedValidation(announcement, "id", "may not be null", NotNull.class);
    }

    @Test
    public void testValidatesNullType()
    {
        DynamicServiceAnnouncement announcement = new DynamicServiceAnnouncement(UUID.randomUUID(), null, "pool", Collections.<String, String>emptyMap());
        assertFailedValidation(announcement, "type", "may not be null", NotNull.class);
    }

    @Test
    public void testValidatesNullPool()
    {
        DynamicServiceAnnouncement announcement = new DynamicServiceAnnouncement(UUID.randomUUID(), "type", null, Collections.<String, String>emptyMap());
        assertFailedValidation(announcement, "pool", "may not be null", NotNull.class);
    }

    @Test
    public void testValidatesNullProperties()
    {
        DynamicServiceAnnouncement announcement = new DynamicServiceAnnouncement(UUID.randomUUID(), "type", "pool", null);
        assertFailedValidation(announcement, "properties", "may not be null", NotNull.class);
    }

    @Test
    public void testParsing()
            throws IOException
    {
        JsonCodec<DynamicServiceAnnouncement> codec = JsonCodec.jsonCodec(DynamicServiceAnnouncement.class);

        DynamicServiceAnnouncement parsed = codec.fromJson(Resources.toString(Resources.getResource("service-announcement.json"), Charsets.UTF_8));
        DynamicServiceAnnouncement expected = new DynamicServiceAnnouncement(UUID.fromString("ff824508-b6a6-4dfc-8f0b-85028465534d"), "blue", "poolA", ImmutableMap.of("key", "valueA"));

        assertEquals(parsed, expected);
    }

    @Test
    public void testEquivalence()
    {
        equivalenceTester()
                // vary fields, one by one
                .addEquivalentGroup(new DynamicServiceAnnouncement(UUID.fromString("ff824508-b6a6-4dfc-8f0b-85028465534d"), "blue", "poolA", ImmutableMap.of("key", "valueA")),
                                    new DynamicServiceAnnouncement(UUID.fromString("ff824508-b6a6-4dfc-8f0b-85028465534d"), "blue", "poolA", ImmutableMap.of("key", "valueA")))
                .addEquivalentGroup(new DynamicServiceAnnouncement(UUID.fromString("ff824508-b6a6-4dfc-8f0b-85028465534d"), "blue", "poolA", ImmutableMap.of("key", "valueB")),
                                    new DynamicServiceAnnouncement(UUID.fromString("ff824508-b6a6-4dfc-8f0b-85028465534d"), "blue", "poolA", ImmutableMap.of("key", "valueB")))
                .addEquivalentGroup(new DynamicServiceAnnouncement(UUID.fromString("ff824508-b6a6-4dfc-8f0b-85028465534d"), "blue", "poolB", ImmutableMap.of("key", "valueA")),
                                    new DynamicServiceAnnouncement(UUID.fromString("ff824508-b6a6-4dfc-8f0b-85028465534d"), "blue", "poolB", ImmutableMap.of("key", "valueA")))
                .addEquivalentGroup(new DynamicServiceAnnouncement(UUID.fromString("ff824508-b6a6-4dfc-8f0b-85028465534d"), "red", "poolA", ImmutableMap.of("key", "valueA")),
                                    new DynamicServiceAnnouncement(UUID.fromString("ff824508-b6a6-4dfc-8f0b-85028465534d"), "red", "poolA", ImmutableMap.of("key", "valueA")))
                .addEquivalentGroup(new DynamicServiceAnnouncement(UUID.fromString("4960d071-67b0-4552-8b12-b7abd869aa83"), "blue", "poolA", ImmutableMap.of("key", "valueA")),
                                    new DynamicServiceAnnouncement(UUID.fromString("4960d071-67b0-4552-8b12-b7abd869aa83"), "blue", "poolA", ImmutableMap.of("key", "valueA")))
                        // null fields
                .addEquivalentGroup(new DynamicServiceAnnouncement(UUID.fromString("ff824508-b6a6-4dfc-8f0b-85028465534d"), "blue", "poolA", null),
                                    new DynamicServiceAnnouncement(UUID.fromString("ff824508-b6a6-4dfc-8f0b-85028465534d"), "blue", "poolA", null))
                .addEquivalentGroup(new DynamicServiceAnnouncement(UUID.fromString("ff824508-b6a6-4dfc-8f0b-85028465534d"), "blue", null, ImmutableMap.of("key", "valueA")),
                                    new DynamicServiceAnnouncement(UUID.fromString("ff824508-b6a6-4dfc-8f0b-85028465534d"), "blue", null, ImmutableMap.of("key", "valueA")))
                .addEquivalentGroup(new DynamicServiceAnnouncement(UUID.fromString("ff824508-b6a6-4dfc-8f0b-85028465534d"), null, "poolA", ImmutableMap.of("key", "valueA")),
                                    new DynamicServiceAnnouncement(UUID.fromString("ff824508-b6a6-4dfc-8f0b-85028465534d"), null, "poolA", ImmutableMap.of("key", "valueA")))
                .addEquivalentGroup(new DynamicServiceAnnouncement(null, "blue", "poolA", ImmutableMap.of("key", "valueA")),
                                    new DynamicServiceAnnouncement(null, "blue", "poolA", ImmutableMap.of("key", "valueA")))

                        // empty properties
                .addEquivalentGroup(new DynamicServiceAnnouncement(UUID.fromString("ff824508-b6a6-4dfc-8f0b-85028465534d"), "blue", "poolA", Collections.<String, String>emptyMap()),
                                    new DynamicServiceAnnouncement(UUID.fromString("ff824508-b6a6-4dfc-8f0b-85028465534d"), "blue", "poolA", Collections.<String, String>emptyMap()))
                .check();
    }

    @Test
    public void testCreatesDefensiveCopyOfProperties()
    {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("key", "value");
        DynamicServiceAnnouncement announcement = new DynamicServiceAnnouncement(UUID.randomUUID(), "type", "pool", properties);

        assertEquals(announcement.getProperties(), properties);
        properties.put("key2", "value2");
        assertNotEquals(announcement.getProperties(), properties);
    }

    @Test
    public void testImmutableProperties()
    {
        DynamicServiceAnnouncement announcement = new DynamicServiceAnnouncement (UUID.randomUUID(), "type", "pool", ImmutableMap.of("key", "value"));

        try {
            announcement.getProperties().put("key2", "value2");

            // a copy of the internal map is acceptable
            assertEquals(announcement.getProperties(), ImmutableMap.of("key", "value"));
        }
        catch (UnsupportedOperationException e) {
            // an exception is ok, too
        }
    }

    private void assertFailedValidation(DynamicServiceAnnouncement announcement, String field, String message, Class<? extends Annotation> annotation)
    {
        Set<ConstraintViolation<DynamicServiceAnnouncement>> violations = VALIDATOR.validate(announcement);
        assertEquals(violations.size(), 1);

        ConstraintViolation<DynamicServiceAnnouncement> violation = violations.iterator().next();
        assertInstanceOf(violation.getConstraintDescriptor().getAnnotation(), annotation);
        assertEquals(violation.getPropertyPath().toString(), field);
        assertEquals(violation.getMessage(), message);
    }

}
