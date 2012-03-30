package com.proofpoint.discovery.store;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.proofpoint.discovery.client.ServiceDescriptor;
import com.proofpoint.discovery.client.ServiceSelector;
import com.proofpoint.http.client.HttpClient;
import com.proofpoint.http.client.Request;
import com.proofpoint.http.client.RequestBuilder;
import com.proofpoint.http.client.Response;
import com.proofpoint.http.client.ResponseHandler;
import com.proofpoint.log.Logger;
import com.proofpoint.node.NodeInfo;
import com.proofpoint.units.Duration;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.smile.SmileFactory;
import org.codehaus.jackson.type.TypeReference;
import org.weakref.jmx.Managed;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.io.EOFException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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
            executor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("replicator-" + name + "-%d").setDaemon(true).build());

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
            }, 0, (long) replicationInterval.toMillis(), TimeUnit.MILLISECONDS);
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
            Request request = RequestBuilder.prepareGet()
                    .setUri(URI.create(uri + "/v1/store/" + name))
                    .build();

            try {
                httpClient.execute(request, new ResponseHandler<Void, Exception>()
                {
                    @Override
                    public Exception handleException(Request request, Exception exception)
                    {
                        return exception;
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
