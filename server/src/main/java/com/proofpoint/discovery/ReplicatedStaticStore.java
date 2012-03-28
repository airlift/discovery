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

public class ReplicatedStaticStore
    implements StaticStore
{
    private final JsonCodec<Service> codec = JsonCodec.jsonCodec(Service.class);
    private final DistributedStore store;

    @Inject
    public ReplicatedStaticStore(@ForStaticStore DistributedStore store)
    {
        this.store = store;
    }

    @Override
    public void put(Service service)
    {
        byte[] key = service.getId().toString().getBytes(UTF_8);
        byte[] value = codec.toJson(service).getBytes(UTF_8);

        store.put(key, value);
    }

    @Override
    public void delete(Id<Service> id)
    {
        byte[] key = id.toString().getBytes(UTF_8);

        store.delete(key);
    }

    @Override
    public Set<Service> getAll()
    {
        ImmutableSet.Builder<Service> builder = ImmutableSet.builder();
        for (Entry entry : store.getAll()) {
            builder.add(codec.fromJson(new String(entry.getValue(), Charsets.UTF_8)));
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
