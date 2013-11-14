package io.airlift.discovery.store;

import io.airlift.json.JsonCodec;
import org.testng.annotations.Test;

import static io.airlift.json.JsonCodec.jsonCodec;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.testng.Assert.assertEquals;

public class TestEntry
{
    @Test
    public void testEntry()
    {
        Entry entry = entryOf("fruit", "apple", 123, 456);

        assertEquals(entry.getKey(), "fruit".getBytes(UTF_8));
        assertEquals(entry.getValue(), "apple".getBytes(UTF_8));
        assertEquals(entry.getVersion().getSequence(), 123);
        assertEquals(entry.getTimestamp(), 456);
        assertEquals(entry.getMaxAgeInMs(), null);
    }

    @Test
    public void testSerialization()
            throws Exception
    {
        JsonCodec<Entry> codec = jsonCodec(Entry.class);

        Entry expected = entryOf("fruit", "apple", 123, 456);
        Entry actual = codec.fromJson(codec.toJsonBytes(expected));

        assertEquals(actual, expected);
    }

    private static Entry entryOf(String key, String value, long version, long timestamp)
    {
        return new Entry(key.getBytes(UTF_8), value.getBytes(UTF_8), new Version(version), timestamp, null);
    }
}
