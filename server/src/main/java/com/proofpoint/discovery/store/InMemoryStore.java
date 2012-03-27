package com.proofpoint.discovery.store;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.concurrent.ConcurrentMap;

import static com.proofpoint.discovery.store.Version.Occurs.AFTER;
import static com.proofpoint.discovery.store.Version.Occurs.SAME;

public class InMemoryStore
{
    private final HashMap<ByteBuffer, Entry> map = new HashMap<ByteBuffer, Entry>();
    private final ConflictResolver resolver;

    @Inject
    public InMemoryStore(ConflictResolver resolver)
    {
        this.resolver = resolver;
    }

    public void put(Entry entry)
    {
        ByteBuffer key = ByteBuffer.wrap(entry.getKey());

        synchronized (map) {
            Entry old = map.get(key);

            if (old != null) {
                entry = resolver.resolve(old, entry);
            }

            ConcurrentMap x;

            map.put(key, entry);
        }
    }

    public Entry get(byte[] key)
    {
        Preconditions.checkNotNull(key, "key is null");

        synchronized (map) {
            return map.get(ByteBuffer.wrap(key));
        }
    }

    public void delete(byte[] key, Version version)
    {
        Preconditions.checkNotNull(key, "key is null");

        synchronized (map) {
            ByteBuffer wrappedKey = ByteBuffer.wrap(key);
            
            Entry old = map.get(wrappedKey);
            if (old != null && EnumSet.of(AFTER, SAME).contains(version.compare(old.getVersion()))) {
                map.remove(wrappedKey);
            }
        }
    }

    public Iterable<Entry> getAll()
    {
        synchronized (map) {
            return ImmutableList.copyOf(map.values());
        }
    }
}
