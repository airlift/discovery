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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;


public class TestId
{
    @Test
    public void testToJson()
    {
        Holder holder = new Holder(Id.<Holder>random());
        Map<String, String> expected = ImmutableMap.of("id", holder.getId().toString());

        String json = jsonCodec(Holder.class).toJson(holder);

        assertEquals(jsonCodec(Object.class).fromJson(json), expected);
    }

    @Test
    public void testFromJson()
    {
        Id<Holder> id = Id.valueOf("9e9b8190-6abd-4890-bc12-e290ebe20a7f");

        Map<String, String> map = ImmutableMap.of("id", id.toString());
        String json = jsonCodec(Object.class).toJson(map);

        assertEquals(jsonCodec(Holder.class).fromJson(json).getId(), id);
    }

    public static class Holder
    {
        private final Id<Holder> id;

        @JsonCreator
        public Holder(@JsonProperty("id") Id<Holder> id)
        {
            this.id = id;
        }

        @JsonProperty
        public Id<Holder> getId()
        {
            return id;
        }
    }
}
