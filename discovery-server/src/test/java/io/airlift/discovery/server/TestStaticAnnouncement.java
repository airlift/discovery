/*
 * Copyright 2010 Proofpoint, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.airlift.discovery.server;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.io.Resources;
import io.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import jakarta.validation.constraints.NotNull;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import static io.airlift.testing.Assertions.assertNotEquals;
import static io.airlift.testing.EquivalenceTester.equivalenceTester;
import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static org.testng.Assert.assertEquals;

public class TestStaticAnnouncement
{
    @Test
    public void testValidatesNullEnvironment()
    {
        StaticAnnouncement announcement = new StaticAnnouncement(null, "type", "pool", "location", Collections.emptyMap());
        assertFailsValidation(announcement, "environment", "must not be null", NotNull.class);
    }

    @Test
    public void testValidatesNullType()
    {
        StaticAnnouncement announcement = new StaticAnnouncement("environment", null, "pool", "location", Collections.emptyMap());
        assertFailsValidation(announcement, "type", "must not be null", NotNull.class);
    }

    @Test
    public void testValidatesNullPool()
    {
        StaticAnnouncement announcement = new StaticAnnouncement("environment", "type", null, "location", Collections.emptyMap());
        assertFailsValidation(announcement, "pool", "must not be null", NotNull.class);
    }

    @Test
    public void testValidatesNullProperties()
    {
        StaticAnnouncement announcement = new StaticAnnouncement("environment", "type", "pool", "location", null);
        assertFailsValidation(announcement, "properties", "must not be null", NotNull.class);
    }

    @Test
    public void testParsing()
            throws IOException
    {
        JsonCodec<StaticAnnouncement> codec = JsonCodec.jsonCodec(StaticAnnouncement.class);

        StaticAnnouncement parsed = codec.fromJson(Resources.toString(Resources.getResource("static-announcement.json"), Charsets.UTF_8));
        StaticAnnouncement expected = new StaticAnnouncement("testing", "blue", "poolA", "/a/b/c", ImmutableMap.of("key", "valueA"));

        assertEquals(parsed, expected);
    }

    @Test
    public void testEquivalence()
    {
        equivalenceTester()
                // vary fields, one by one
                .addEquivalentGroup(
                        new StaticAnnouncement("testing", "blue", "poolA", "/a/b", ImmutableMap.of("key", "valueA")),
                        new StaticAnnouncement("testing", "blue", "poolA", "/a/b", ImmutableMap.of("key", "valueA")))
                .addEquivalentGroup(
                        new StaticAnnouncement("testing", "blue", "poolA", "/a/b", ImmutableMap.of("key", "valueB")),
                        new StaticAnnouncement("testing", "blue", "poolA", "/a/b", ImmutableMap.of("key", "valueB")))
                .addEquivalentGroup(
                        new StaticAnnouncement("testing", "blue", "poolB", "/a/b", ImmutableMap.of("key", "valueA")),
                        new StaticAnnouncement("testing", "blue", "poolB", "/a/b", ImmutableMap.of("key", "valueA")))
                .addEquivalentGroup(
                        new StaticAnnouncement("testing", "red", "poolA", "/a/b", ImmutableMap.of("key", "valueA")),
                        new StaticAnnouncement("testing", "red", "poolA", "/a/b", ImmutableMap.of("key", "valueA")))
                .addEquivalentGroup(
                        new StaticAnnouncement("testing", "red", "poolA", "/x/y", ImmutableMap.of("key", "valueA")),
                        new StaticAnnouncement("testing", "red", "poolA", "/x/y", ImmutableMap.of("key", "valueA")))
                .addEquivalentGroup(
                        new StaticAnnouncement("production", "blue", "poolA", "/a/b", ImmutableMap.of("key", "valueA")),
                        new StaticAnnouncement("production", "blue", "poolA", "/a/b", ImmutableMap.of("key", "valueA")))
                // null fields
                .addEquivalentGroup(
                        new StaticAnnouncement("testing", "blue", "poolA", "/a/b", null),
                        new StaticAnnouncement("testing", "blue", "poolA", "/a/b", null))
                .addEquivalentGroup(
                        new StaticAnnouncement("testing", "blue", null, "/a/b", ImmutableMap.of("key", "valueA")),
                        new StaticAnnouncement("testing", "blue", null, "/a/b", ImmutableMap.of("key", "valueA")))
                .addEquivalentGroup(
                        new StaticAnnouncement("testing", null, "poolA", "/a/b", ImmutableMap.of("key", "valueA")),
                        new StaticAnnouncement("testing", null, "poolA", "/a/b", ImmutableMap.of("key", "valueA")))
                .addEquivalentGroup(
                        new StaticAnnouncement("testing", "blue", "poolA", null, ImmutableMap.of("key", "valueA")),
                        new StaticAnnouncement("testing", "blue", "poolA", null, ImmutableMap.of("key", "valueA")))
                .addEquivalentGroup(
                        new StaticAnnouncement(null, "blue", "poolA", "/a/b", ImmutableMap.of("key", "valueA")),
                        new StaticAnnouncement(null, "blue", "poolA", "/a/b", ImmutableMap.of("key", "valueA")))
                // empty properties
                .addEquivalentGroup(
                        new StaticAnnouncement("testing", "blue", "poolA", "/a/b", Collections.emptyMap()),
                        new StaticAnnouncement("testing", "blue", "poolA", "/a/b", Collections.emptyMap()))
                .check();
    }

    @Test
    public void testCreatesDefensiveCopyOfProperties()
    {
        Map<String, String> properties = Maps.newHashMap();
        properties.put("key", "value");
        StaticAnnouncement announcement = new StaticAnnouncement("testing", "type", "pool", "/a/b", properties);

        assertEquals(announcement.getProperties(), properties);
        properties.put("key2", "value2");
        assertNotEquals(announcement.getProperties(), properties);
    }

    @Test
    public void testImmutableProperties()
    {
        StaticAnnouncement announcement = new StaticAnnouncement("testing", "type", "pool", "/a/b", ImmutableMap.of("key", "value"));

        try {
            announcement.getProperties().put("key2", "value2");

            // a copy of the internal map is acceptable
            assertEquals(announcement.getProperties(), ImmutableMap.of("key", "value"));
        }
        catch (UnsupportedOperationException e) {
            // an exception is ok, too
        }
    }
}
