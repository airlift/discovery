package com.proofpoint.discovery;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;
import com.proofpoint.json.JsonCodec;
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

import static com.proofpoint.testing.Assertions.assertInstanceOf;
import static com.proofpoint.testing.Assertions.assertNotEquals;
import static com.proofpoint.testing.EquivalenceTester.equivalenceTester;
import static org.testng.Assert.assertEquals;

public class TestStaticAnnouncement
{
    private static final Validator VALIDATOR = Validation.buildDefaultValidatorFactory().getValidator();

    @Test
    public void testValidatesNullEnvironment()
    {
        StaticAnnouncement announcement = new StaticAnnouncement(null, "location", "type", "pool", Collections.<String, String>emptyMap());
        assertFailedValidation(announcement, "environment", "may not be null", NotNull.class);
    }

    @Test
    public void testValidatesNullType()
    {
        StaticAnnouncement announcement = new StaticAnnouncement("environment", "location", null, "pool", Collections.<String, String>emptyMap());
        assertFailedValidation(announcement, "type", "may not be null", NotNull.class);
    }

    @Test
    public void testValidatesNullPool()
    {
        StaticAnnouncement announcement = new StaticAnnouncement("environment", "location", "type", null, Collections.<String, String>emptyMap());
        assertFailedValidation(announcement, "pool", "may not be null", NotNull.class);
    }

    @Test
    public void testValidatesNullProperties()
    {
        StaticAnnouncement announcement = new StaticAnnouncement("environment", "location", "type", "pool", null);
        assertFailedValidation(announcement, "properties", "may not be null", NotNull.class);
    }

    @Test
    public void testParsing()
            throws IOException
    {
        JsonCodec<StaticAnnouncement> codec = JsonCodec.jsonCodec(StaticAnnouncement.class);

        StaticAnnouncement parsed = codec.fromJson(Resources.toString(Resources.getResource("static-announcement.json"), Charsets.UTF_8));
        StaticAnnouncement expected = new StaticAnnouncement("testing", "/a/b/c", "blue", "poolA", ImmutableMap.of("key", "valueA"));

        assertEquals(parsed, expected);
    }

    @Test
    public void testEquivalence()
    {
        equivalenceTester()
                // vary fields, one by one
                .addEquivalentGroup(new StaticAnnouncement("testing", "/a/b", "blue", "poolA", ImmutableMap.of("key", "valueA")),
                                    new StaticAnnouncement("testing", "/a/b", "blue", "poolA", ImmutableMap.of("key", "valueA")))
                .addEquivalentGroup(new StaticAnnouncement("testing", "/a/b", "blue", "poolA", ImmutableMap.of("key", "valueB")),
                                    new StaticAnnouncement("testing", "/a/b", "blue", "poolA", ImmutableMap.of("key", "valueB")))
                .addEquivalentGroup(new StaticAnnouncement("testing", "/a/b", "blue", "poolB", ImmutableMap.of("key", "valueA")),
                                    new StaticAnnouncement("testing", "/a/b", "blue", "poolB", ImmutableMap.of("key", "valueA")))
                .addEquivalentGroup(new StaticAnnouncement("testing", "/a/b", "red", "poolA", ImmutableMap.of("key", "valueA")),
                                    new StaticAnnouncement("testing", "/a/b", "red", "poolA", ImmutableMap.of("key", "valueA")))
                .addEquivalentGroup(new StaticAnnouncement("testing", "/x/y", "red", "poolA", ImmutableMap.of("key", "valueA")),
                                    new StaticAnnouncement("testing", "/x/y", "red", "poolA", ImmutableMap.of("key", "valueA")))
                .addEquivalentGroup(new StaticAnnouncement("production", "/a/b", "blue", "poolA", ImmutableMap.of("key", "valueA")),
                                    new StaticAnnouncement("production", "/a/b", "blue", "poolA", ImmutableMap.of("key", "valueA")))
                        // null fields
                .addEquivalentGroup(new StaticAnnouncement("testing", "/a/b", "blue", "poolA", null),
                                    new StaticAnnouncement("testing", "/a/b", "blue", "poolA", null))
                .addEquivalentGroup(new StaticAnnouncement("testing", "/a/b", "blue", null, ImmutableMap.of("key", "valueA")),
                                    new StaticAnnouncement("testing", "/a/b", "blue", null, ImmutableMap.of("key", "valueA")))
                .addEquivalentGroup(new StaticAnnouncement("testing", "/a/b", null, "poolA", ImmutableMap.of("key", "valueA")),
                                    new StaticAnnouncement("testing", "/a/b", null, "poolA", ImmutableMap.of("key", "valueA")))
                .addEquivalentGroup(new StaticAnnouncement("testing", null, "blue", "poolA", ImmutableMap.of("key", "valueA")),
                                    new StaticAnnouncement("testing", null, "blue", "poolA", ImmutableMap.of("key", "valueA")))
                .addEquivalentGroup(new StaticAnnouncement(null, "/a/b", "blue", "poolA", ImmutableMap.of("key", "valueA")),
                                    new StaticAnnouncement(null, "/a/b", "blue", "poolA", ImmutableMap.of("key", "valueA")))

                        // empty properties
                .addEquivalentGroup(new StaticAnnouncement("testing", "/a/b", "blue", "poolA", Collections.<String, String>emptyMap()),
                                    new StaticAnnouncement("testing", "/a/b", "blue", "poolA", Collections.<String, String>emptyMap()))
                .check();
    }

    @Test
    public void testCreatesDefensiveCopyOfProperties()
    {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("key", "value");
        StaticAnnouncement announcement = new StaticAnnouncement("testing", "/a/b", "type", "pool", properties);

        assertEquals(announcement.getProperties(), properties);
        properties.put("key2", "value2");
        assertNotEquals(announcement.getProperties(), properties);
    }

    @Test
    public void testImmutableProperties()
    {
        StaticAnnouncement announcement = new StaticAnnouncement("testing", "/a/b", "type", "pool", ImmutableMap.of("key", "value"));

        try {
            announcement.getProperties().put("key2", "value2");

            // a copy of the internal map is acceptable
            assertEquals(announcement.getProperties(), ImmutableMap.of("key", "value"));
        }
        catch (UnsupportedOperationException e) {
            // an exception is ok, too
        }
    }

    private void assertFailedValidation(StaticAnnouncement announcement, String field, String message, Class<? extends Annotation> annotation)
    {
        Set<ConstraintViolation<StaticAnnouncement>> violations = VALIDATOR.validate(announcement);
        assertEquals(violations.size(), 1);

        ConstraintViolation<StaticAnnouncement> violation = violations.iterator().next();
        assertInstanceOf(violation.getConstraintDescriptor().getAnnotation(), annotation);
        assertEquals(violation.getPropertyPath().toString(), field);
        assertEquals(violation.getMessage(), message);
    }
}
