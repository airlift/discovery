package com.proofpoint.discovery;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
import com.proofpoint.discovery.store.DistributedStore;
import com.proofpoint.discovery.store.Entry;
import com.proofpoint.json.JsonCodec;
import com.proofpoint.units.Duration;

import javax.inject.Inject;
import java.util.List;
import java.util.Set;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Predicates.and;
import static com.google.common.collect.ImmutableList.copyOf;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static com.proofpoint.discovery.DynamicServiceAnnouncement.toServiceWith;
import static com.proofpoint.discovery.Service.matchesPool;
import static com.proofpoint.discovery.Service.matchesType;

public class ReplicatedDynamicStore
    implements DynamicStore
{
    private final JsonCodec<List<Service>> codec = JsonCodec.listJsonCodec(Service.class);

    private final DistributedStore store;
    private final Duration maxAge;

    @Inject
    public ReplicatedDynamicStore(@ForDynamicStore DistributedStore store, DiscoveryConfig config)
    {
        this.store = store;
        this.maxAge = config.getMaxAge();
    }

    @Override
    public boolean put(Id<Node> nodeId, DynamicAnnouncement announcement)
    {
        List<Service> services = copyOf(transform(announcement.getServiceAnnouncements(), toServiceWith(nodeId, announcement.getLocation(), announcement.getPool())));

        byte[] key = nodeId.toString().getBytes(UTF_8);
        byte[] value = codec.toJson(services).getBytes(UTF_8);

        store.put(key, value, maxAge);

        return true; // TODO
    }

    @Override
    public boolean delete(Id<Node> nodeId)
    {
        byte[] key = nodeId.toString().getBytes(UTF_8);

        store.delete(key);

        return true; // TODO
    }

    @Override
    public Set<Service> getAll()
    {
        ImmutableSet.Builder<Service> builder = ImmutableSet.builder();
        for (Entry entry : store.getAll()) {
            builder.addAll(codec.fromJson(new String(entry.getValue(), Charsets.UTF_8)));
        }

        return builder.build();
    }

    @Override
    public Set<Service> get(String type)
    {
        return ImmutableSet.copyOf(filter(getAll(), matchesType(type)));
    }

    @Override
    public Set<Service> get(String type, String pool)
    {
        return ImmutableSet.copyOf(filter(getAll(), and(matchesType(type), matchesPool(pool))));
    }
}
