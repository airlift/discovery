package com.proofpoint.discovery.store;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.proofpoint.discovery.RealTimeProvider;
import com.proofpoint.http.client.HttpClientModule;
import org.joda.time.DateTime;

import static com.proofpoint.configuration.ConfigurationModule.bindConfig;

public class ReplicatedStoreModule
        implements Module
{
    public void configure(Binder binder)
    {
        binder.requireExplicitBindings();
        binder.disableCircularProxies();

        binder.bind(Replicator.class).in(Scopes.SINGLETON);
        binder.bind(StoreResource.class).in(Scopes.SINGLETON);
        binder.bind(InMemoryStore.class).in(Scopes.SINGLETON);
        binder.bind(DistributedStore.class).in(Scopes.SINGLETON);
        binder.bind(RemoteStore.class).to(HttpRemoteStore.class).in(Scopes.SINGLETON);
        binder.bind(ConflictResolver.class).in(Scopes.SINGLETON);
        binder.bind(DateTime.class).toProvider(RealTimeProvider.class);

        binder.install(new HttpClientModule(ForRemoteStoreClient.class));

        bindConfig(binder).to(StoreConfig.class);

        binder.bind(SmileMapper.class).in(Scopes.SINGLETON);
    }
}
