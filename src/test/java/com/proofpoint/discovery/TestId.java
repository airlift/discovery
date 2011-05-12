package com.proofpoint.discovery;

import com.google.common.collect.ImmutableMap;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.testng.annotations.Test;

import java.util.Map;

import static com.proofpoint.json.JsonCodec.jsonCodec;
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
