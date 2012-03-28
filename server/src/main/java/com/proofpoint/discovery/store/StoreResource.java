package com.proofpoint.discovery.store;

import com.google.common.base.Charsets;
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

@Path("/v1/store")
public class StoreResource
{
    private final LocalStore localStore;
    private final Duration tombstoneMaxAge;

    @Inject
    public StoreResource(LocalStore localStore, StoreConfig config)
    {
        this.localStore = localStore;

        tombstoneMaxAge = config.getTombstoneMaxAge();
    }

    @PUT
    @Path("{key}")
    public void put(@PathParam("key") String key, byte[] value)
    {
        Entry entry = new Entry(key.getBytes(Charsets.UTF_8), value, new Version(System.currentTimeMillis()), System.currentTimeMillis(), null); // TODO: version
        localStore.put(entry);
    }
    
    @POST
    @Consumes({"application/x-jackson-smile", "application/json"})
    public void setMultipleEntries(List<Entry> entries)
    {
        for (Entry entry : entries) {
            if (!isExpired(entry)) {
                localStore.put(entry);
            }
        }
    }

    @GET
    @Produces({"application/x-jackson-smile", "application/json"})
    public Iterable<Entry> getAll()
    {
        return localStore.getAll();
    }

    private boolean isExpired(Entry entry)
    {
        long ageInMs = System.currentTimeMillis() - entry.getTimestamp();

        return entry.getValue() == null && ageInMs > tombstoneMaxAge.toMillis() ||
                entry.getMaxAgeInMs() != null && ageInMs > entry.getMaxAgeInMs();
    }
}
