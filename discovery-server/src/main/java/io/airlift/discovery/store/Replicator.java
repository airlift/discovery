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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import io.airlift.discovery.client.ServiceDescriptor;
import io.airlift.discovery.client.ServiceSelector;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;
import io.airlift.units.Duration;
import org.weakref.jmx.Managed;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.EOFException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class Replicator
{
    private final static Logger log = Logger.get(Replicator.class);

    private final String name;
    private final NodeInfo node;
    private final ServiceSelector selector;
    private final HttpClient httpClient;
    private final LocalStore localStore;
    private final Duration replicationInterval;

    private ScheduledFuture<?> future;
    private ScheduledExecutorService executor;

    private final ObjectMapper mapper = new ObjectMapper(new SmileFactory());
    private final AtomicLong lastReplicationTimestamp = new AtomicLong();

    @Inject
    public Replicator(String name,
            NodeInfo node,
            ServiceSelector selector,
            HttpClient httpClient,
            LocalStore localStore,
            StoreConfig config)
    {
        this.name = name;
        this.node = node;
        this.selector = selector;
        this.httpClient = httpClient;
        this.localStore = localStore;

        this.replicationInterval = config.getReplicationInterval();

    }

    @PostConstruct
    public synchronized void start()
    {
        if (future == null) {
            executor = newSingleThreadScheduledExecutor(daemonThreadsNamed("replicator-" + name + "-%d"));

            future = executor.scheduleAtFixedRate(new Runnable()
            {
                @Override
                public void run()
                {
                    try {
                        synchronize();
                    }
                    catch (Throwable t) {
                        log.warn(t, "Error replicating state");
                    }
                }
            }, 0, replicationInterval.toMillis(), TimeUnit.MILLISECONDS);
        }

        // TODO: need fail-safe recurrent scheduler with variable delay
    }

    @PreDestroy
    public synchronized void shutdown()
    {
        if (future != null) {
            future.cancel(true);
            executor.shutdownNow();

            executor = null;
            future = null;
        }
    }

    @Managed
    public long getLastReplicationTimestamp()
    {
        return lastReplicationTimestamp.get();
    }

    private void synchronize()
    {
        for (ServiceDescriptor descriptor : selector.selectAllServices()) {
            if (descriptor.getNodeId().equals(node.getNodeId())) {
                // don't write to ourselves
                continue;
            }

            String uri = descriptor.getProperties().get("http");
            if (uri == null) {
                log.error("service descriptor for node %s is missing http uri", descriptor.getNodeId());
                continue;
            }

            // TODO: build URI from resource class
            Request request = Request.Builder.prepareGet()
                    .setUri(URI.create(uri + "/v1/store/" + name))
                    .build();

            try {
                httpClient.execute(request, new ResponseHandler<Void, Exception>()
                {
                    @Override
                    public Void handleException(Request request, Exception exception)
                            throws Exception
                    {
                        throw exception;
                    }

                    @Override
                    public Void handle(Request request, Response response)
                            throws Exception
                    {
                        // TODO: read server date (to use to calibrate entry dates)


                        if (response.getStatusCode() == 200) {
                            try {
                                List<Entry> entries = mapper.readValue(response.getInputStream(), new TypeReference<List<Entry>>() {});
                                for (Entry entry : entries) {
                                    localStore.put(entry);
                                }
                            }
                            catch (EOFException e) {
                                // ignore
                            }
                        }

                        return null;
                    }
                });
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            catch (Exception e) {
                // ignore
            }
        }

        lastReplicationTimestamp.set(System.currentTimeMillis());
    }
}
