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

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import io.airlift.units.Duration;
import jakarta.annotation.Nullable;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.PUT;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;

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
