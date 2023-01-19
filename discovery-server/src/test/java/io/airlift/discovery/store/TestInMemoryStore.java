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
package io.airlift.discovery.store;

import com.google.common.base.Charsets;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.google.common.base.Charsets.UTF_8;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestInMemoryStore
{
    private LocalStore store;

    @BeforeMethod
    protected void setUp()
            throws Exception
    {
        store = new InMemoryStore(new ConflictResolver());
    }

    @Test
    public void testPut()
    {
        Entry entry = entryOf("blue", "apple", 1, 0);
        store.put(entry);

        assertEquals(store.get("blue".getBytes(UTF_8)), entry);
    }

    @Test
    public void testDelete()
    {
        byte[] key = "blue".getBytes(UTF_8);
        Entry entry = entryOf("blue", "apple", 1, 0);
        store.put(entry);

        store.delete(key, entry.getVersion());

        assertNull(store.get(key));
    }

    @Test
    public void testDeleteOlderVersion()
    {
        byte[] key = "blue".getBytes(UTF_8);
        Entry entry = entryOf("blue", "apple", 5, 0);
        store.put(entry);

        store.delete(key, new Version(2));

        assertEquals(store.get("blue".getBytes(UTF_8)), entry);
    }

    @Test
    public void testResolvesConflict()
    {
        Entry entry2 = entryOf("blue", "apple", 2, 0);
        store.put(entry2);

        Entry entry1 = entryOf("blue", "banana", 1, 0);
        store.put(entry1);

        assertEquals(store.get("blue".getBytes(UTF_8)), entry2);
    }

    private static Entry entryOf(String key, String value, long version, long timestamp)
    {
        return new Entry(key.getBytes(UTF_8), value.getBytes(UTF_8), new Version(version), timestamp, null);
    }
}
