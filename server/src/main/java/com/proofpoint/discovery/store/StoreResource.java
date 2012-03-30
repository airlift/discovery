package com.proofpoint.discovery.store;

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.proofpoint.units.Duration;

import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.util.List;
import java.util.Map;

@Path("/v1/store/{store}")
public class StoreResource
{
    private final Map<String, LocalStore> localStores;
    private final Map<String, Duration> tombstoneMaxAges;

    @Inject
    public StoreResource(Map<String, LocalStore> localStores, Map<String, StoreConfig> configs)
    {
        this.localStores = ImmutableMap.copyOf(localStores);
        this.tombstoneMaxAges = ImmutableMap.copyOf(Maps.transformValues(configs, new Function<StoreConfig, Duration>()
        {
            @Override
            public Duration apply(@Nullable StoreConfig config)
            {
                return config.getTombstoneMaxAge();
            }
        }));
    }

    @PUT
    @Path("{key}")
    public void put(@PathParam("store") String storeName, @PathParam("key") String key, byte[] value)
    {
        LocalStore store = localStores.get(storeName);

        Entry entry = new Entry(key.getBytes(Charsets.UTF_8), value, new Version(System.currentTimeMillis()), System.currentTimeMillis(), null); // TODO: version
        store.put(entry);
    }
    
    @POST
    @Consumes({"application/x-jackson-smile", "application/json"})
    public Response setMultipleEntries(@PathParam("store") String storeName, List<Entry> entries)
    {
        LocalStore store = localStores.get(storeName);
        Duration tombstoneMaxAge = tombstoneMaxAges.get(storeName);
        if (store == null || tombstoneMaxAge == null) {
            return Response.status(Status.NOT_FOUND).build();
        }

        for (Entry entry : entries) {
            if (!isExpired(tombstoneMaxAge, entry)) {
                store.put(entry);
            }
        }
        return Response.noContent().build();
    }

    @GET
    @Produces({"application/x-jackson-smile", "application/json"})
    public Response getAll(@PathParam("store") String storeName)
    {
        LocalStore store = localStores.get(storeName);
        if (store == null) {
            return Response.status(Status.NOT_FOUND).build();
        }
        return Response.ok(store.getAll()).build();
    }

    private boolean isExpired(Duration tombstoneMaxAge, Entry entry)
    {
        long ageInMs = System.currentTimeMillis() - entry.getTimestamp();

        return entry.getValue() == null && ageInMs > tombstoneMaxAge.toMillis() ||
                entry.getMaxAgeInMs() != null && ageInMs > entry.getMaxAgeInMs();
    }
}
