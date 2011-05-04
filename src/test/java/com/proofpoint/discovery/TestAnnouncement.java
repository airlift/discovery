package com.proofpoint.discovery;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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
import static org.testng.Assert.assertTrue;

public class TestAnnouncement
{
    private static final Validator VALIDATOR = Validation.buildDefaultValidatorFactory().getValidator();

    @Test
    public void testRejectsNullEnvironment()
    {
        Announcement announcement = new Announcement(null, "/location", Collections.<ServiceAnnouncement>emptySet());
        assertFailedValidation(announcement, "environment", "may not be null", NotNull.class);
    }

    @Test
    public void testAllowsNullLocation()
    {
        Announcement announcement = new Announcement("testing", null, Collections.<ServiceAnnouncement>emptySet());

        Set<ConstraintViolation<Announcement>> violations = VALIDATOR.validate(announcement);
        assertTrue(violations.isEmpty());
    }

    @Test
    public void testRejectsNullServiceAnnouncements()
    {
        Announcement announcement = new Announcement("testing", "/location", null);
        assertFailedValidation(announcement, "services", "may not be null", NotNull.class);
    }

    @Test
    public void testValidatesNestedServiceAnnouncements()
    {
        Set<ServiceAnnouncement> serviceAnnouncements = ImmutableSet.of(new ServiceAnnouncement(null, "type", "pool", Collections.<String, String>emptyMap()));
        Announcement announcement = new Announcement("testing", "/location", serviceAnnouncements);

        assertFailedValidation(announcement, "services[].id", "may not be null", NotNull.class);
    }

    @Test
    public void testParsing()
            throws IOException
    {
        JsonCodec<ServiceAnnouncement> codec = JsonCodec.jsonCodec(ServiceAnnouncement.class);

        ServiceAnnouncement parsed = codec.fromJson(Resources.toString(Resources.getResource("service-announcement.json"), Charsets.UTF_8));
        ServiceAnnouncement expected = new ServiceAnnouncement(UUID.fromString("ff824508-b6a6-4dfc-8f0b-85028465534d"), "blue", "poolA", ImmutableMap.of("key", "valueA"));

        assertEquals(parsed, expected);
    }

    @Test
    public void testEquivalence()
    {
        equivalenceTester()
                // vary fields, one by one
                .addEquivalentGroup(new ServiceAnnouncement(UUID.fromString("ff824508-b6a6-4dfc-8f0b-85028465534d"), "blue", "poolA", ImmutableMap.of("key", "valueA")),
                                    new ServiceAnnouncement(UUID.fromString("ff824508-b6a6-4dfc-8f0b-85028465534d"), "blue", "poolA", ImmutableMap.of("key", "valueA")))
                .addEquivalentGroup(new ServiceAnnouncement(UUID.fromString("ff824508-b6a6-4dfc-8f0b-85028465534d"), "blue", "poolA", ImmutableMap.of("key", "valueB")),
                                    new ServiceAnnouncement(UUID.fromString("ff824508-b6a6-4dfc-8f0b-85028465534d"), "blue", "poolA", ImmutableMap.of("key", "valueB")))
                .addEquivalentGroup(new ServiceAnnouncement(UUID.fromString("ff824508-b6a6-4dfc-8f0b-85028465534d"), "blue", "poolB", ImmutableMap.of("key", "valueA")),
                                    new ServiceAnnouncement(UUID.fromString("ff824508-b6a6-4dfc-8f0b-85028465534d"), "blue", "poolB", ImmutableMap.of("key", "valueA")))
                .addEquivalentGroup(new ServiceAnnouncement(UUID.fromString("ff824508-b6a6-4dfc-8f0b-85028465534d"), "red", "poolA", ImmutableMap.of("key", "valueA")),
                                    new ServiceAnnouncement(UUID.fromString("ff824508-b6a6-4dfc-8f0b-85028465534d"), "red", "poolA", ImmutableMap.of("key", "valueA")))
                .addEquivalentGroup(new ServiceAnnouncement(UUID.fromString("4960d071-67b0-4552-8b12-b7abd869aa83"), "blue", "poolA", ImmutableMap.of("key", "valueA")),
                                    new ServiceAnnouncement(UUID.fromString("4960d071-67b0-4552-8b12-b7abd869aa83"), "blue", "poolA", ImmutableMap.of("key", "valueA")))
                        // null fields
                .addEquivalentGroup(new ServiceAnnouncement(UUID.fromString("ff824508-b6a6-4dfc-8f0b-85028465534d"), "blue", "poolA", null),
                                    new ServiceAnnouncement(UUID.fromString("ff824508-b6a6-4dfc-8f0b-85028465534d"), "blue", "poolA", null))
                .addEquivalentGroup(new ServiceAnnouncement(UUID.fromString("ff824508-b6a6-4dfc-8f0b-85028465534d"), "blue", null, ImmutableMap.of("key", "valueA")),
                                    new ServiceAnnouncement(UUID.fromString("ff824508-b6a6-4dfc-8f0b-85028465534d"), "blue", null, ImmutableMap.of("key", "valueA")))
                .addEquivalentGroup(new ServiceAnnouncement(UUID.fromString("ff824508-b6a6-4dfc-8f0b-85028465534d"), null, "poolA", ImmutableMap.of("key", "valueA")),
                                    new ServiceAnnouncement(UUID.fromString("ff824508-b6a6-4dfc-8f0b-85028465534d"), null, "poolA", ImmutableMap.of("key", "valueA")))
                .addEquivalentGroup(new ServiceAnnouncement(null, "blue", "poolA", ImmutableMap.of("key", "valueA")),
                                    new ServiceAnnouncement(null, "blue", "poolA", ImmutableMap.of("key", "valueA")))

                        // empty properties
                .addEquivalentGroup(new ServiceAnnouncement(UUID.fromString("ff824508-b6a6-4dfc-8f0b-85028465534d"), "blue", "poolA", Collections.<String, String>emptyMap()),
                                    new ServiceAnnouncement(UUID.fromString("ff824508-b6a6-4dfc-8f0b-85028465534d"), "blue", "poolA", Collections.<String, String>emptyMap()))
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

    private <T> void assertFailedValidation(T bean, String field, String message, Class<? extends Annotation> annotation)
    {
        Set<ConstraintViolation<T>> violations = VALIDATOR.validate(bean);
        assertEquals(violations.size(), 1);

        ConstraintViolation<T> violation = violations.iterator().next();
        assertInstanceOf(violation.getConstraintDescriptor().getAnnotation(), annotation);
        assertEquals(violation.getPropertyPath().toString(), field);
        assertEquals(violation.getMessage(), message);
    }

}
