package com.proofpoint.discovery.store;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.proofpoint.units.Duration;

import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import java.util.List;
import java.util.Map;

@Path("/v1/store/{store}")
public class StoreResource
{
    private final Map<String, LocalStore> localStores;
    private final Map<String, StoreConfig> configs;

    @Inject
    public StoreResource(Map<String, LocalStore> localStores, Map<String, StoreConfig> configs)
    {
        // TODO: this class should not need store configs -- LocalStore should encapsulate any necessary logic
        this.configs = ImmutableMap.copyOf(configs);
        this.localStores = ImmutableMap.copyOf(localStores);
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
    public void setMultipleEntries(@PathParam("store") String storeName, List<Entry> entries)
    {
        LocalStore store = localStores.get(storeName);
        StoreConfig config = configs.get(storeName);

        for (Entry entry : entries) {
            if (!isExpired(config.getTombstoneMaxAge(), entry)) {
                store.put(entry);
            }
        }
    }

    @GET
    @Produces({"application/x-jackson-smile", "application/json"})
    public Iterable<Entry> getAll(@PathParam("store") String storeName)
    {
        LocalStore store = localStores.get(storeName);
        return store.getAll();
    }

    private boolean isExpired(Duration tombstoneMaxAge, Entry entry)
    {
        long ageInMs = System.currentTimeMillis() - entry.getTimestamp();

        return entry.getValue() == null && ageInMs > tombstoneMaxAge.toMillis() ||
                entry.getMaxAgeInMs() != null && ageInMs > entry.getMaxAgeInMs();
    }
}
