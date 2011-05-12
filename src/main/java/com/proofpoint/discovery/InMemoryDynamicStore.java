package com.proofpoint.discovery;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.proofpoint.units.Duration;
import org.joda.time.DateTime;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;
import javax.inject.Provider;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Predicates.and;
import static com.google.common.collect.Collections2.transform;
import static com.google.common.collect.Iterables.filter;
import static com.proofpoint.discovery.DynamicServiceAnnouncement.toServiceWith;
import static com.proofpoint.discovery.Service.matchesPool;
import static com.proofpoint.discovery.Service.matchesType;

@ThreadSafe
public class InMemoryDynamicStore
        implements DynamicStore
{
    private final Map<Id<Node>, Entry> descriptors = Maps.newHashMap();
    private final Duration maxAge;
    private final Provider<DateTime> currentTime;

    @Inject
    public InMemoryDynamicStore(DiscoveryConfig config, Provider<DateTime> timeSource)
    {
        this.currentTime = timeSource;
        this.maxAge = config.getMaxAge();
    }

    @Override
    public synchronized boolean put(Id<Node> nodeId, DynamicAnnouncement announcement)
    {
        Preconditions.checkNotNull(nodeId, "nodeId is null");
        Preconditions.checkNotNull(announcement, "announcement is null");

        Set<Service> services = ImmutableSet.copyOf(transform(announcement.getServiceAnnouncements(), toServiceWith(nodeId, announcement.getLocation(), announcement.getPool())));

        DateTime expiration = currentTime.get().plusMillis((int) maxAge.toMillis());
        Entry old = descriptors.put(nodeId, new Entry(expiration, services));

        return old == null || old.getExpiration().isBefore(currentTime.get());
    }

    @Override
    public synchronized boolean delete(Id<Node> nodeId)
    {
        Preconditions.checkNotNull(nodeId, "nodeId is null");

        return descriptors.remove(nodeId) != null;
    }

    @Override
    public synchronized Set<Service> getAll()
    {
        removeExpired();

        ImmutableSet.Builder<Service> builder = ImmutableSet.builder();
        for (Entry entry : descriptors.values()) {
            builder.addAll(entry.getServices());
        }
        return builder.build();
    }

    @Override
    public synchronized Set<Service> get(String type)
    {
        Preconditions.checkNotNull(type, "type is null");

        return ImmutableSet.copyOf(filter(getAll(), matchesType(type)));
    }

    @Override
    public synchronized Set<Service> get(String type, String pool)
    {
        Preconditions.checkNotNull(type, "type is null");
        Preconditions.checkNotNull(pool, "pool is null");

        return ImmutableSet.copyOf(filter(getAll(), and(matchesType(type), matchesPool(pool))));
    }

    private synchronized void removeExpired()
    {
        Iterator<Entry> iterator = descriptors.values().iterator();

        DateTime now = currentTime.get();
        while (iterator.hasNext()) {
            Entry entry = iterator.next();

            if (now.isAfter(entry.getExpiration())) {
                iterator.remove();
            }
        }
    }

    private static class Entry
    {
        private final Set<Service> services;
        private final DateTime expiration;

        public Entry(DateTime expiration, Set<Service> services)
        {
            this.expiration = expiration;
            this.services = ImmutableSet.copyOf(services);
        }

        public DateTime getExpiration()
        {
            return expiration;
        }

        public Set<Service> getServices()
        {
            return services;
        }
    }
}
