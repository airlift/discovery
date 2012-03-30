package com.proofpoint.discovery.store;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.proofpoint.discovery.client.ServiceDescriptor;
import com.proofpoint.discovery.client.ServiceSelector;
import com.proofpoint.http.client.BodyGenerator;
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
import org.weakref.jmx.MBeanExporter;
import org.weakref.jmx.Managed;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.inject.Inject;
import java.io.OutputStream;
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
import java.util.concurrent.Executors;
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
import static org.weakref.jmx.ObjectNames.generatedNameOf;

public class HttpRemoteStore
        implements RemoteStore
{
    private final static Logger log = Logger.get(HttpRemoteStore.class);

    private final int maxBatchSize;
    private final int queueSize;
    private final Duration updateInterval;

    private final ConcurrentMap<String, BatchProcessor<Entry>> processors = new ConcurrentHashMap<String, BatchProcessor<Entry>>();
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
            executor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("http-remote-store-" + name + "-%d").setDaemon(true).build());

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
            }, 0, (long) updateInterval.toMillis(), TimeUnit.MILLISECONDS);
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
                executor.submit(new Runnable() {
                    public void run() {
                        updateProcessors(Collections.<ServiceDescriptor>emptyList());
                    }
                }).get();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            catch (ExecutionException e) {
                Throwables.propagate(e);
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
        private final ObjectMapper mapper = new ObjectMapper(new SmileFactory());

        private final URI uri;
        private final HttpClient httpClient;

        public MyBatchHandler(String name, ServiceDescriptor descriptor, HttpClient httpClient)
        {
            this.httpClient = httpClient;

            // TODO: build URI from resource class
            uri = URI.create(descriptor.getProperties().get("http") + "/v1/store/" + name);
        }

        @Override
        public void processBatch(final Collection<Entry> entries)
        {
            final Request request = RequestBuilder.preparePost()
                    .setUri(uri)
                    .setHeader("Content-Type", "application/x-jackson-smile")
                    .setBodyGenerator(new BodyGenerator() {
                        @Override
                        public void write(OutputStream out)
                                throws Exception
                        {
                            mapper.writeValue(out, entries);
                        }
                    })
                    .build();

            try {
                httpClient.execute(request, new ResponseHandler<Void, Exception>()
                {
                    @Override
                    public Exception handleException(Request request, Exception exception)
                    {
                        // ignore
                        return exception;
                    }

                    @Override
                    public Void handle(Request request, Response response)
                            throws Exception
                    {
                        // ignore
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
    }
}
