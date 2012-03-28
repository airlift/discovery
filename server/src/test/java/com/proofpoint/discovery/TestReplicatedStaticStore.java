package com.proofpoint.discovery;

import com.proofpoint.discovery.store.ConflictResolver;
import com.proofpoint.discovery.store.DistributedStore;
import com.proofpoint.discovery.store.Entry;
import com.proofpoint.discovery.store.InMemoryStore;
import com.proofpoint.discovery.store.RemoteStore;
import com.proofpoint.discovery.store.StoreConfig;
import org.joda.time.DateTime;

import javax.inject.Provider;

public class TestReplicatedStaticStore
    extends TestStaticStore
{
    @Override
    protected StaticStore initializeStore(Provider<DateTime> timeProvider)
    {
        RemoteStore dummy = new RemoteStore() {
            public void put(Entry entry) { }
        };

        DistributedStore distributedStore = new DistributedStore("static", new InMemoryStore(new ConflictResolver()), dummy, new StoreConfig(), timeProvider);

        return new ReplicatedStaticStore(distributedStore);
    }
}
