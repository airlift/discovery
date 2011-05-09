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

public class TestDynamicAnnouncement
{
    private static final Validator VALIDATOR = Validation.buildDefaultValidatorFactory().getValidator();

    @Test
    public void testRejectsNullEnvironment()
    {
        DynamicAnnouncement announcement = new DynamicAnnouncement(null, "/location", Collections.<DynamicServiceAnnouncement>emptySet());
        assertFailedValidation(announcement, "environment", "may not be null", NotNull.class);
    }

    @Test
    public void testAllowsNullLocation()
    {
        DynamicAnnouncement announcement = new DynamicAnnouncement("testing", null, Collections.<DynamicServiceAnnouncement>emptySet());

        Set<ConstraintViolation<DynamicAnnouncement>> violations = VALIDATOR.validate(announcement);
        assertTrue(violations.isEmpty());
    }

    @Test
    public void testRejectsNullServiceAnnouncements()
    {
        DynamicAnnouncement announcement = new DynamicAnnouncement("testing", "/location", null);
        assertFailedValidation(announcement, "services", "may not be null", NotNull.class);
    }

    @Test
    public void testValidatesNestedServiceAnnouncements()
    {
        Set<DynamicServiceAnnouncement> serviceAnnouncements = ImmutableSet.of(new DynamicServiceAnnouncement(null, "type", "pool", Collections.<String, String>emptyMap()));
        DynamicAnnouncement announcement = new DynamicAnnouncement("testing", "/location", serviceAnnouncements);

        assertFailedValidation(announcement, "services[].id", "may not be null", NotNull.class);
    }

    @Test
    public void testParsing()
            throws IOException
    {
        JsonCodec<DynamicAnnouncement> codec = JsonCodec.jsonCodec(DynamicAnnouncement.class);

        DynamicAnnouncement parsed = codec.fromJson(Resources.toString(Resources.getResource("announcement.json"), Charsets.UTF_8));

        DynamicServiceAnnouncement red = new DynamicServiceAnnouncement(UUID.fromString("1c001650-7841-11e0-a1f0-0800200c9a66"), "red", "poolA", ImmutableMap.of("key", "redValue"));
        DynamicServiceAnnouncement blue = new DynamicServiceAnnouncement(UUID.fromString("2a817750-7841-11e0-a1f0-0800200c9a66"), "blue", "poolA", ImmutableMap.of("key", "blueValue"));
        DynamicAnnouncement expected = new DynamicAnnouncement("testing", "/a/b/c", ImmutableSet.of(red, blue));

        assertEquals(parsed, expected);
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
