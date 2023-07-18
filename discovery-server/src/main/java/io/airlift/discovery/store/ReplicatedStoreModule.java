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

import com.google.common.base.Supplier;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.http.client.HttpClient;
import io.airlift.node.NodeInfo;
import jakarta.annotation.PreDestroy;
import org.joda.time.DateTime;
import org.weakref.jmx.MBeanExporter;

import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.errorprone.annotations.ThreadSafe;

import java.lang.annotation.Annotation;

import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static com.google.inject.name.Names.named;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.airlift.http.client.HttpClientBinder.httpClientBinder;
import static io.airlift.jaxrs.JaxrsBinder.jaxrsBinder;
import static org.weakref.jmx.ObjectNames.generatedNameOf;
import static org.weakref.jmx.guice.ExportBinder.newExporter;

/**
 * Expects a LocalStore to be bound elsewhere.
 * Provides a DistributedStore with the specified annotation.
 */
public class ReplicatedStoreModule
        implements Module
{
    private final String name;
    private final Class<? extends Annotation> annotation;
    private final Class<? extends LocalStore> localStoreClass;

    public ReplicatedStoreModule(String name, Class<? extends Annotation> annotation, Class<? extends LocalStore> localStoreClass)
    {
        this.name = name;
        this.annotation = annotation;
        this.localStoreClass = localStoreClass;
    }

    @Override
    public void configure(Binder binder)
    {
        binder.requireExplicitBindings();
        binder.disableCircularProxies();

        // global
        jaxrsBinder(binder).bind(StoreResource.class);
        binder.bind(new TypeLiteral<Supplier<DateTime>>() {}).to(RealTimeSupplier.class).in(Scopes.SINGLETON);
        binder.bind(ConflictResolver.class).in(Scopes.SINGLETON);

        // per store
        Key<HttpClient> httpClientKey = Key.get(HttpClient.class, annotation);
        Key<LocalStore> localStoreKey = Key.get(LocalStore.class, annotation);
        Key<StoreConfig> storeConfigKey = Key.get(StoreConfig.class, annotation);
        Key<RemoteStore> remoteStoreKey = Key.get(RemoteStore.class, annotation);

        configBinder(binder).bindConfig(StoreConfig.class, annotation, name);
        httpClientBinder(binder).bindHttpClient(name, annotation);

        binder.bind(DistributedStore.class).annotatedWith(annotation).toProvider(new DistributedStoreProvider(name, localStoreKey, storeConfigKey, remoteStoreKey)).in(Scopes.SINGLETON);
        binder.bind(Replicator.class).annotatedWith(annotation).toProvider(new ReplicatorProvider(name, localStoreKey, httpClientKey, storeConfigKey)).in(Scopes.SINGLETON);
        binder.bind(HttpRemoteStore.class).annotatedWith(annotation).toProvider(new RemoteHttpStoreProvider(name, httpClientKey, storeConfigKey)).in(Scopes.SINGLETON);
        binder.bind(LocalStore.class).annotatedWith(annotation).to(localStoreClass).in(Scopes.SINGLETON);

        binder.bind(RemoteStore.class).annotatedWith(annotation).to(Key.get(HttpRemoteStore.class, annotation));

        newExporter(binder).export(DistributedStore.class).annotatedWith(annotation).as(generatedNameOf(DistributedStore.class, named(name)));
        newExporter(binder).export(HttpRemoteStore.class).annotatedWith(annotation).as(generatedNameOf(HttpRemoteStore.class, named(name)));
        newExporter(binder).export(Replicator.class).annotatedWith(annotation).as(generatedNameOf(Replicator.class, named(name)));

        newMapBinder(binder, String.class, LocalStore.class)
                .addBinding(name)
                .to(localStoreKey);

        newMapBinder(binder, String.class, StoreConfig.class)
                .addBinding(name)
                .to(storeConfigKey);
    }

    @ThreadSafe
    private static class ReplicatorProvider
            implements Provider<Replicator>
    {
        private final String name;
        private final Key<? extends LocalStore> localStoreKey;
        private final Key<? extends HttpClient> httpClientKey;
        private final Key<StoreConfig> storeConfigKey;

        @GuardedBy("this")
        private Injector injector;

        @GuardedBy("this")
        private NodeInfo nodeInfo;

        @GuardedBy("this")
        private ServiceSelector serviceSelector;

        @GuardedBy("this")
        private Replicator replicator;

        private ReplicatorProvider(String name, Key<? extends LocalStore> localStoreKey, Key<? extends HttpClient> httpClientKey, Key<StoreConfig> storeConfigKey)
        {
            this.name = name;
            this.localStoreKey = localStoreKey;
            this.httpClientKey = httpClientKey;
            this.storeConfigKey = storeConfigKey;
        }

        @Override
        public synchronized Replicator get()
        {
            if (replicator == null) {
                LocalStore localStore = injector.getInstance(localStoreKey);
                HttpClient httpClient = injector.getInstance(httpClientKey);
                StoreConfig storeConfig = injector.getInstance(storeConfigKey);

                replicator = new Replicator(name, nodeInfo, serviceSelector, httpClient, localStore, storeConfig);
                replicator.start();
            }

            return replicator;
        }

        @PreDestroy
        public synchronized void shutdown()
        {
            if (replicator != null) {
                replicator.shutdown();
            }
        }

        @Inject
        public synchronized void setInjector(Injector injector)
        {
            this.injector = injector;
        }

        @Inject
        public synchronized void setNodeInfo(NodeInfo nodeInfo)
        {
            this.nodeInfo = nodeInfo;
        }

        @Inject
        public synchronized void setServiceSelector(ServiceSelector serviceSelector)
        {
            this.serviceSelector = serviceSelector;
        }
    }

    @ThreadSafe
    private static class RemoteHttpStoreProvider
            implements Provider<HttpRemoteStore>
    {
        @GuardedBy("this")
        private HttpRemoteStore remoteStore;

        @GuardedBy("this")
        private Injector injector;

        @GuardedBy("this")
        private NodeInfo nodeInfo;

        @GuardedBy("this")
        private ServiceSelector serviceSelector;

        @GuardedBy("this")
        private MBeanExporter mbeanExporter;

        private final String name;
        private final Key<? extends HttpClient> httpClientKey;
        private final Key<StoreConfig> storeConfigKey;

        @Inject
        private RemoteHttpStoreProvider(String name, Key<? extends HttpClient> httpClientKey, Key<StoreConfig> storeConfigKey)
        {
            this.name = name;
            this.httpClientKey = httpClientKey;
            this.storeConfigKey = storeConfigKey;
        }

        @Override
        public synchronized HttpRemoteStore get()
        {
            if (remoteStore == null) {
                HttpClient httpClient = injector.getInstance(httpClientKey);
                StoreConfig storeConfig = injector.getInstance(storeConfigKey);

                remoteStore = new HttpRemoteStore(name, nodeInfo, serviceSelector, storeConfig, httpClient, mbeanExporter);
                remoteStore.start();
            }

            return remoteStore;
        }

        @PreDestroy
        public synchronized void shutdown()
        {
            if (remoteStore != null) {
                remoteStore.shutdown();
            }
        }

        @Inject
        public synchronized void setInjector(Injector injector)
        {
            this.injector = injector;
        }

        @Inject
        public synchronized void setNodeInfo(NodeInfo nodeInfo)
        {
            this.nodeInfo = nodeInfo;
        }

        @Inject
        public synchronized void setServiceSelector(ServiceSelector serviceSelector)
        {
            this.serviceSelector = serviceSelector;
        }

        @Inject
        public synchronized void setMbeanExporter(MBeanExporter mbeanExporter)
        {
            this.mbeanExporter = mbeanExporter;
        }
    }

    private static class DistributedStoreProvider
            implements Provider<DistributedStore>
    {
        private final String name;
        private final Key<? extends LocalStore> localStoreKey;
        private final Key<StoreConfig> storeConfigKey;
        private final Key<? extends RemoteStore> remoteStoreKey;

        private Injector injector;
        private Supplier<DateTime> timeSupplier;
        private DistributedStore store;

        public DistributedStoreProvider(String name,
                Key<? extends LocalStore> localStoreKey,
                Key<StoreConfig> storeConfigKey,
                Key<? extends RemoteStore> remoteStoreKey)
        {
            this.name = name;
            this.localStoreKey = localStoreKey;
            this.storeConfigKey = storeConfigKey;
            this.remoteStoreKey = remoteStoreKey;
        }

        @Override
        public synchronized DistributedStore get()
        {
            if (store == null) {
                LocalStore localStore = injector.getInstance(localStoreKey);
                StoreConfig storeConfig = injector.getInstance(storeConfigKey);
                RemoteStore remoteStore = injector.getInstance(remoteStoreKey);

                store = new DistributedStore(name, localStore, remoteStore, storeConfig, timeSupplier);
                store.start();
            }

            return store;
        }

        @PreDestroy
        public synchronized void shutdown()
        {
            if (store != null) {
                store.shutdown();
            }
        }

        @Inject
        public synchronized void setInjector(Injector injector)
        {
            this.injector = injector;
        }

        @Inject
        public synchronized void setTimeSupplier(Supplier<DateTime> timeSupplier)
        {
            this.timeSupplier = timeSupplier;
        }
    }
}
