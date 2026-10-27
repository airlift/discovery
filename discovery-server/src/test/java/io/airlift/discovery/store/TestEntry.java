/*
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
package io.airlift.discovery.store;

import io.airlift.json.JsonCodec;
import org.junit.jupiter.api.Test;

import static io.airlift.json.JsonCodec.jsonCodec;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class TestEntry
{
    @Test
    public void testEntry()
    {
        Entry entry = entryOf("fruit", "apple", 123, 456, 3000L);

        assertThat(entry.getKey()).isEqualTo("fruit".getBytes(UTF_8));
        assertThat(entry.getValue()).isEqualTo("apple".getBytes(UTF_8));
        assertThat(entry.getVersion().getSequence()).isEqualTo(123);
        assertThat(entry.getTimestamp()).isEqualTo(456);
        assertThat(entry.getMaxAgeInMs()).isEqualTo(Long.valueOf(3000L));
    }

    @Test
    public void testSerialization()
            throws Exception
    {
        JsonCodec<Entry> codec = jsonCodec(Entry.class);

        Entry expected = entryOf("fruit", "apple", 123, 456, 3000L);
        Entry actual = codec.fromJson(codec.toJsonBytes(expected));

        assertThat(actual).isEqualTo(expected);
    }

    private static Entry entryOf(String key, String value, long version, long timestamp, Long maxAgeInMs)
    {
        return new Entry(key.getBytes(UTF_8), value.getBytes(UTF_8), new Version(version), timestamp, maxAgeInMs);
    }
}
