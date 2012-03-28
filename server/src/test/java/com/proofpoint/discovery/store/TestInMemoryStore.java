package com.proofpoint.discovery.store;

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

        assertEquals(store.get("blue".getBytes(Charsets.UTF_8)), entry);
    }

    @Test
    public void testDelete()
    {
        byte[] key = "blue".getBytes(Charsets.UTF_8);
        Entry entry = entryOf("blue", "apple", 1, 0);
        store.put(entry);

        store.delete(key, entry.getVersion());

        assertNull(store.get(key));
    }

    @Test
    public void testDeleteOlderVersion()
    {
        byte[] key = "blue".getBytes(Charsets.UTF_8);
        Entry entry = entryOf("blue", "apple", 5, 0);
        store.put(entry);

        store.delete(key, new Version(2));

        assertEquals(store.get("blue".getBytes(Charsets.UTF_8)), entry);
    }

    @Test
    public void testResolvesConflict()
    {
        Entry entry2 = entryOf("blue", "apple", 2, 0);
        store.put(entry2);

        Entry entry1 = entryOf("blue", "banana", 1, 0);
        store.put(entry1);

        assertEquals(store.get("blue".getBytes(Charsets.UTF_8)), entry2);
    }

    private static Entry entryOf(String key, String value, long version, long timestamp)
    {
        return new Entry(key.getBytes(UTF_8), value.getBytes(Charsets.UTF_8), new Version(version), timestamp, null);
    }
}
