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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;
import com.google.inject.Inject;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.airlift.units.Duration;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.Managed;

import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Predicates.and;
import static com.google.common.base.Predicates.compose;
import static com.google.common.base.Predicates.equalTo;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static com.google.inject.name.Names.named;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.weakref.jmx.ObjectNames.generatedNameOf;

public class HttpRemoteStore
        implements RemoteStore
{
    private static final Logger log = Logger.get(HttpRemoteStore.class);

    private final int maxBatchSize;
    private final int queueSize;
    private final Duration updateInterval;

    private final ConcurrentMap<String, BatchProcessor<Entry>> processors = new ConcurrentHashMap<>();
    private final String name;
    private final NodeInfo node;
    private final ServiceSelector selector;
    private final HttpClient httpClient;

    private Future<?> future;
    private ScheduledExecutorService executor;

    private final AtomicLong lastRemoteServerRefreshTimestamp = new AtomicLong();
    private final MBeanExporter mbeanExporter;

    @Inject
    public HttpRemoteStore(String name,
            NodeInfo node,
            ServiceSelector selector,
            StoreConfig config,
            HttpClient httpClient,
            MBeanExporter mbeanExporter)
    {
        Preconditions.checkNotNull(name, "name is null");
        Preconditions.checkNotNull(node, "node is null");
        Preconditions.checkNotNull(selector, "selector is null");
        Preconditions.checkNotNull(httpClient, "httpClient is null");
        Preconditions.checkNotNull(config, "config is null");
        Preconditions.checkNotNull(mbeanExporter, "mBeanExporter is null");

        this.name = name;
        this.node = node;
        this.selector = selector;
        this.httpClient = httpClient;
        this.mbeanExporter = mbeanExporter;

        maxBatchSize = config.getMaxBatchSize();
        queueSize = config.getQueueSize();
        updateInterval = config.getRemoteUpdateInterval();
    }

    @PostConstruct
    public synchronized void start()
    {
        if (future == null) {
            // note: this *must* be single threaded for the shutdown logic to work correctly
            executor = newSingleThreadScheduledExecutor(daemonThreadsNamed("http-remote-store-" + name));

            future = executor.scheduleWithFixedDelay(new Runnable()
            {
                @Override
                public void run()
                {
                    try {
                        updateProcessors(selector.selectAllServices());
                    }
                    catch (Throwable e) {
                        log.warn(e, "Error refreshing batch processors");
                    }
                }
            }, 0, updateInterval.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    @PreDestroy
    public synchronized void shutdown()
    {
        if (future != null) {
            future.cancel(true);

            try {
                // schedule a task to shut down all processors and wait for it to complete. We rely on the executor
                // having a *single* thread to guarantee the execution happens after any currently running task
                // (in case the cancel call above didn't do its magic and the scheduled task is still running)
                executor.submit(new Runnable()
                {
                    @Override
                    public void run()
                    {
                        updateProcessors(Collections.emptyList());
                    }
                }).get();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            catch (ExecutionException e) {
                throw new RuntimeException(e);
            }

            executor.shutdownNow();

            future = null;
        }
    }

    private void updateProcessors(List<ServiceDescriptor> descriptors)
    {
        Set<String> nodeIds = ImmutableSet.copyOf(transform(descriptors, getNodeIdFunction()));

        // remove old ones
        Iterator<Map.Entry<String, BatchProcessor<Entry>>> iterator = processors.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, BatchProcessor<Entry>> entry = iterator.next();

            if (!nodeIds.contains(entry.getKey())) {
                iterator.remove();
                entry.getValue().stop();
                mbeanExporter.unexport(nameFor(entry.getKey()));
            }
        }

        Predicate<ServiceDescriptor> predicate = compose(and(not(equalTo(node.getNodeId())), not(in(processors.keySet()))), getNodeIdFunction());
        Iterable<ServiceDescriptor> newDescriptors = filter(descriptors, predicate);

        for (ServiceDescriptor descriptor : newDescriptors) {
            BatchProcessor<Entry> processor = new BatchProcessor<Entry>(descriptor.getNodeId(),
                    new MyBatchHandler(name, descriptor, httpClient),
                    maxBatchSize,
                    queueSize);

            processor.start();
            processors.put(descriptor.getNodeId(), processor);
            mbeanExporter.export(nameFor(descriptor.getNodeId()), processor);
        }

        lastRemoteServerRefreshTimestamp.set(System.currentTimeMillis());
    }

    private String nameFor(String id)
    {
        return generatedNameOf(BatchProcessor.class, named(name + "-" + id));
    }

    @Managed
    public long getLastRemoteServerRefreshTimestamp()
    {
        return lastRemoteServerRefreshTimestamp.get();
    }

    private static Function<ServiceDescriptor, String> getNodeIdFunction()
    {
        return new Function<ServiceDescriptor, String>()
        {
            @Override
            public String apply(ServiceDescriptor descriptor)
            {
                return descriptor.getNodeId();
            }
        };
    }

    @Override
    public void put(Entry entry)
    {
        for (BatchProcessor<Entry> processor : processors.values()) {
            processor.put(entry);
        }
    }

    private static class MyBatchHandler
            implements BatchProcessor.BatchHandler<Entry>
    {
        private static final ObjectMapper SMILE_MAPPER = new ObjectMapper(new SmileFactory());
        private static final JsonCodec<Collection<Entry>> CODEC = new JsonCodecFactory(() -> SMILE_MAPPER).jsonCodec(new TypeToken<>() {});

        private final URI uri;
        private final HttpClient httpClient;

        public MyBatchHandler(String name, ServiceDescriptor descriptor, HttpClient httpClient)
        {
            this.httpClient = httpClient;

            // TODO: build URI from resource class
            uri = URI.create(descriptor.getProperties().get("http") + "/v1/store/" + name);
        }

        @Override
        public void processBatch(Collection<Entry> entries)
        {
            Request request = Request.Builder.preparePost()
                    .setUri(uri)
                    .setHeader("Content-Type", "application/x-jackson-smile")
                    .setBodyGenerator(jsonBodyGenerator(CODEC, entries))
                    .build();

            try {
                httpClient.execute(request, createStatusResponseHandler());
            }
            catch (RuntimeException e) {
                // ignore
            }
        }
    }
}
