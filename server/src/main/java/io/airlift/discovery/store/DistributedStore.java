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
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.airlift.units.Duration;
import org.joda.time.DateTime;
import org.weakref.jmx.Managed;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.Arrays;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Predicates.and;
import static com.google.common.base.Predicates.not;

/**
 * A simple, eventually consistent, fully replicated, distributed key-value store.
 */
public class DistributedStore
{
    private final String name;
    private final LocalStore localStore;
    private final RemoteStore remoteStore;
    private final Supplier<DateTime> timeSupplier;
    private final Duration tombstoneMaxAge;
    private final Duration garbageCollectionInterval;

    private final ScheduledExecutorService garbageCollector;
    private final AtomicLong lastGcTimestamp = new AtomicLong();

    @Inject
    public DistributedStore(String name, LocalStore localStore, RemoteStore remoteStore, StoreConfig config, Supplier<DateTime> timeSupplier)
    {
        Preconditions.checkNotNull(name, "name is null");
        Preconditions.checkNotNull(localStore, "localStore is null");
        Preconditions.checkNotNull(remoteStore, "remoteStore is null");
        Preconditions.checkNotNull(config, "config is null");
        Preconditions.checkNotNull(timeSupplier, "timeSupplier is null");

        this.name = name;
        this.localStore = localStore;
        this.remoteStore = remoteStore;
        this.timeSupplier = timeSupplier;

        tombstoneMaxAge = config.getTombstoneMaxAge();
        garbageCollectionInterval = config.getGarbageCollectionInterval();
        
        garbageCollector = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("distributed-store-gc-" + name + "-%d").setDaemon(true).build());
    }

    @PostConstruct
    public void start()
    {
        garbageCollector.scheduleAtFixedRate(new Runnable()
        {
            @Override
            public void run()
            {
                removeExpiredEntries();
            }
        }, 0, garbageCollectionInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Managed
    public String getName()
    {
        return name;
    }

    @Managed
    public long getLastGcTimestamp()
    {
        return lastGcTimestamp.get();
    }

    @Managed
    public void removeExpiredEntries()
    {
        for (Entry entry : localStore.getAll()) {
            if (isExpired(entry)) {
                localStore.delete(entry.getKey(), entry.getVersion());
            }
        }

        lastGcTimestamp.set(System.currentTimeMillis());
    }

    private boolean isExpired(Entry entry)
    {
        long ageInMs = timeSupplier.get().getMillis() - entry.getTimestamp();

        return entry.getValue() == null && ageInMs > tombstoneMaxAge.toMillis() ||  // TODO: this is repeated in StoreResource
                entry.getMaxAgeInMs() != null && ageInMs > entry.getMaxAgeInMs();
    }

    @PreDestroy
    public void shutdown()
    {
        garbageCollector.shutdownNow();
    }

    public void put(byte[] key, byte[] value)
    {
        Preconditions.checkNotNull(key, "key is null");
        Preconditions.checkNotNull(value, "value is null");

        long now = timeSupplier.get().getMillis();

        Entry entry = new Entry(key, value, now, null);

        localStore.put(entry);
        remoteStore.put(entry);
    }
    
    public void put(byte[] key, byte[] value, Duration maxAge)
    {
        Preconditions.checkNotNull(key, "key is null");
        Preconditions.checkNotNull(value, "value is null");
        Preconditions.checkNotNull(maxAge, "maxAge is null");

        long now = timeSupplier.get().getMillis();

        Entry entry = new Entry(key, value, now, maxAge.toMillis());

        localStore.put(entry);
        remoteStore.put(entry);
    }

    public byte[] get(byte[] key)
    {
        Preconditions.checkNotNull(key, "key is null");

        Entry entry = localStore.get(key);
        
        byte[] result = null;
        if (entry != null && entry.getValue() != null && !isExpired(entry)) {
            result = Arrays.copyOf(entry.getValue(), entry.getValue().length);
        }

        return result;
    }

    public void delete(byte[] key)
    {
        Preconditions.checkNotNull(key, "key is null");

        long now = timeSupplier.get().getMillis();

        Entry entry = new Entry(key, null, now, null);

        localStore.put(entry);
        remoteStore.put(entry);
    }

    public Iterable<Entry> getAll()
    {
        return Iterables.filter(localStore.getAll(), and(not(expired()), not(tombstone())));
    }

    private Predicate<? super Entry> expired()
    {
        return new Predicate<Entry>()
        {
            public boolean apply(Entry entry)
            {
                return isExpired(entry);
            }
        };
    }

    private Predicate<? super Entry> tombstone()
    {
        return new Predicate<Entry>()
        {
            public boolean apply(Entry entry)
            {
                return entry.getValue() == null;
            }
        };
    }
}
