package com.proofpoint.discovery;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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
import java.util.Set;

import static com.proofpoint.testing.Assertions.assertInstanceOf;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class TestDynamicAnnouncement
{
    private static final Validator VALIDATOR = Validation.buildDefaultValidatorFactory().getValidator();

    @Test
    public void testRejectsNullEnvironment()
    {
        DynamicAnnouncement announcement = new DynamicAnnouncement(null, "/location", "pool", Collections.<DynamicServiceAnnouncement>emptySet());
        assertFailedValidation(announcement, "environment", "may not be null", NotNull.class);
    }

    @Test
    public void testAllowsNullLocation()
    {
        DynamicAnnouncement announcement = new DynamicAnnouncement("testing", null, "pool", Collections.<DynamicServiceAnnouncement>emptySet());

        Set<ConstraintViolation<DynamicAnnouncement>> violations = VALIDATOR.validate(announcement);
        assertTrue(violations.isEmpty());
    }

    @Test
    public void testRejectsNullPool()
    {
        DynamicAnnouncement announcement = new DynamicAnnouncement("testing", "/location", null, Collections.<DynamicServiceAnnouncement>emptySet());
        assertFailedValidation(announcement, "pool", "may not be null", NotNull.class);
    }

    @Test
    public void testRejectsNullServiceAnnouncements()
    {
        DynamicAnnouncement announcement = new DynamicAnnouncement("testing", "/location", "pool", null);
        assertFailedValidation(announcement, "serviceAnnouncements", "may not be null", NotNull.class);
    }

    @Test
    public void testValidatesNestedServiceAnnouncements()
    {
        DynamicAnnouncement announcement = new DynamicAnnouncement("testing", "/location", "pool", ImmutableSet.of(
                new DynamicServiceAnnouncement(null, "type", Collections.<String, String>emptyMap()))
        );

        assertFailedValidation(announcement, "serviceAnnouncements[].id", "may not be null", NotNull.class);
    }

    @Test
    public void testParsing()
            throws IOException
    {
        JsonCodec<DynamicAnnouncement> codec = JsonCodec.jsonCodec(DynamicAnnouncement.class);

        DynamicAnnouncement parsed = codec.fromJson(Resources.toString(Resources.getResource("announcement.json"), Charsets.UTF_8));

        DynamicServiceAnnouncement red = new DynamicServiceAnnouncement(Id.<Service>valueOf("1c001650-7841-11e0-a1f0-0800200c9a66"), "red", ImmutableMap.of("key", "redValue"));
        DynamicServiceAnnouncement blue = new DynamicServiceAnnouncement(Id.<Service>valueOf("2a817750-7841-11e0-a1f0-0800200c9a66"), "blue", ImmutableMap.of("key", "blueValue"));
        DynamicAnnouncement expected = new DynamicAnnouncement("testing", "/a/b/c", "poolA", ImmutableSet.of(red, blue));

        assertEquals(parsed, expected);
    }

    @Test
    public void testToString()
    {
        DynamicAnnouncement announcement = new DynamicAnnouncement("testing", "/location", "pool", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.<Service>random(), "type", Collections.<String, String>emptyMap()))
        );

        assertNotNull(announcement.toString());
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
