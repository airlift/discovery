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

import com.google.common.base.Preconditions;

import javax.inject.Inject;

import java.nio.ByteBuffer;
import java.util.EnumSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static io.airlift.discovery.store.Version.Occurs.AFTER;
import static io.airlift.discovery.store.Version.Occurs.SAME;

public class InMemoryStore
        implements LocalStore
{
    private final ConcurrentMap<ByteBuffer, Entry> map = new ConcurrentHashMap<ByteBuffer, Entry>();
    private final ConflictResolver resolver;

    @Inject
    public InMemoryStore(ConflictResolver resolver)
    {
        this.resolver = resolver;
    }

    @Override
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

    @Override
    public Entry get(byte[] key)
    {
        Preconditions.checkNotNull(key, "key is null");

        return map.get(ByteBuffer.wrap(key));
    }

    @Override
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

    @Override
    public Iterable<Entry> getAll()
    {
        return map.values();
    }
}
