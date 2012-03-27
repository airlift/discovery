package com.proofpoint.discovery.store;

import com.google.common.base.Preconditions;

import javax.inject.Inject;
import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.proofpoint.discovery.store.Version.Occurs.AFTER;
import static com.proofpoint.discovery.store.Version.Occurs.SAME;

public class InMemoryStore
{
    private final ConcurrentMap<ByteBuffer, Entry> map = new ConcurrentHashMap<ByteBuffer, Entry>();
    private final ConflictResolver resolver;

    @Inject
    public InMemoryStore(ConflictResolver resolver)
    {
        this.resolver = resolver;
    }

    public void put(Entry entry)
    {
        ByteBuffer key = ByteBuffer.wrap(entry.getKey());

        boolean done = false;
        while (!done) {
            Entry old = map.putIfAbsent(key, entry);

            done = true;
            if (old != null) {
                entry = resolver.resolve(old, entry);

                if (entry != old) {
                    done = map.replace(key, old, entry);
                }
            }
        }
    }

    public Entry get(byte[] key)
    {
        Preconditions.checkNotNull(key, "key is null");

        return map.get(ByteBuffer.wrap(key));
    }

    public void delete(byte[] key, Version version)
    {
        Preconditions.checkNotNull(key, "key is null");

        ByteBuffer wrappedKey = ByteBuffer.wrap(key);

        boolean done = false;
        while (!done) {
            Entry old = map.get(wrappedKey);

            done = true;
            if (old != null && EnumSet.of(AFTER, SAME).contains(version.compare(old.getVersion()))) {
                done = map.remove(wrappedKey, old);
            }
        }
    }

    public Iterable<Entry> getAll()
    {
        return map.values();
    }
}
