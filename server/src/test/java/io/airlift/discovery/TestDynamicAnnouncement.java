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
package io.airlift.discovery;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import io.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.Collections;

import static io.airlift.testing.ValidationAssertions.assertFailsValidation;
import static io.airlift.testing.ValidationAssertions.assertValidates;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

public class TestDynamicAnnouncement
{
    @Test
    public void testRejectsNullEnvironment()
    {
        DynamicAnnouncement announcement = new DynamicAnnouncement(null, "pool", "/location", Collections.<DynamicServiceAnnouncement>emptySet());
        assertFailsValidation(announcement, "environment", "may not be null", NotNull.class);
    }

    @Test
    public void testAllowsNullLocation()
    {
        DynamicAnnouncement announcement = new DynamicAnnouncement("testing", "pool", null, Collections.<DynamicServiceAnnouncement>emptySet());

        assertValidates(announcement);
    }

    @Test
    public void testRejectsNullPool()
    {
        DynamicAnnouncement announcement = new DynamicAnnouncement("testing", null, "/location", Collections.<DynamicServiceAnnouncement>emptySet());
        assertFailsValidation(announcement, "pool", "may not be null", NotNull.class);
    }

    @Test
    public void testRejectsNullServiceAnnouncements()
    {
        DynamicAnnouncement announcement = new DynamicAnnouncement("testing", "pool", "/location", null);
        assertFailsValidation(announcement, "serviceAnnouncements", "may not be null", NotNull.class);
    }

    @Test
    public void testValidatesNestedServiceAnnouncements()
    {
        DynamicAnnouncement announcement = new DynamicAnnouncement("testing", "pool", "/location", ImmutableSet.of(
                new DynamicServiceAnnouncement(null, "type", Collections.<String, String>emptyMap()))
        );

        assertFailsValidation(announcement, "serviceAnnouncements[].id", "may not be null", NotNull.class);
    }

    @Test
    public void testParsing()
            throws IOException
    {
        JsonCodec<DynamicAnnouncement> codec = JsonCodec.jsonCodec(DynamicAnnouncement.class);

        DynamicAnnouncement parsed = codec.fromJson(Resources.toString(Resources.getResource("announcement.json"), Charsets.UTF_8));

        DynamicServiceAnnouncement red = new DynamicServiceAnnouncement(Id.<Service>valueOf("1c001650-7841-11e0-a1f0-0800200c9a66"), "red", ImmutableMap.of("key", "redValue"));
        DynamicServiceAnnouncement blue = new DynamicServiceAnnouncement(Id.<Service>valueOf("2a817750-7841-11e0-a1f0-0800200c9a66"), "blue", ImmutableMap.of("key", "blueValue"));
        DynamicAnnouncement expected = new DynamicAnnouncement("testing", "poolA", "/a/b/c", ImmutableSet.of(red, blue));

        assertEquals(parsed, expected);
    }

    @Test
    public void testToString()
    {
        DynamicAnnouncement announcement = new DynamicAnnouncement("testing", "pool", "/location", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.<Service>random(), "type", Collections.<String, String>emptyMap()))
        );

        assertNotNull(announcement.toString());
    }
}
