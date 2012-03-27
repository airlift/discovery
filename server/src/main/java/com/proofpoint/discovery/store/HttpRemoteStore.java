package com.proofpoint.discovery.store;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.proofpoint.discovery.client.ServiceDescriptor;
import com.proofpoint.discovery.client.ServiceSelector;
import com.proofpoint.http.client.BodyGenerator;
import com.proofpoint.http.client.HttpClient;
import com.proofpoint.http.client.Request;
import com.proofpoint.http.client.RequestBuilder;
import com.proofpoint.http.client.Response;
import com.proofpoint.http.client.ResponseHandler;
import com.proofpoint.node.NodeInfo;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.smile.SmileFactory;

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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Predicates.and;
import static com.google.common.base.Predicates.compose;
import static com.google.common.base.Predicates.equalTo;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.filter;

public class HttpRemoteStore
        implements RemoteStore
{
    private final int maxBatchSize;
    private final int queueSize;

    private final ConcurrentMap<String, BatchProcessor<Entry>> processors = new ConcurrentHashMap<String, BatchProcessor<Entry>>();
    private final ScheduledExecutorService executor;
    private final NodeInfo node;
    private final ServiceSelector selector;
    private final HttpClient httpClient;

    @Inject
    public HttpRemoteStore(NodeInfo node,
            ServiceSelector selector,
            StoreConfig config,
            @ForRemoteStoreClient HttpClient httpClient)
    {
        this.node = node;
        this.selector = selector;
        this.httpClient = httpClient;

        maxBatchSize = config.getMaxBatchSize();
        queueSize = config.getQueueSize();

        executor = Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setNameFormat("remote-store-%d").setDaemon(true).build());
    }

    @PostConstruct
    public void start()
    {
        executor.scheduleWithFixedDelay(new Runnable()
        {
            @Override
            public void run()
            {
                updateProcessors(selector.selectAllServices());
            }
        }, 0, 5, TimeUnit.SECONDS); // TODO: make configurable
    }

    @PreDestroy
    public void shutdown()
    {
        executor.shutdownNow();
        updateProcessors(Collections.<ServiceDescriptor>emptyList());
    }

    private void updateProcessors(List<ServiceDescriptor> descriptors)
    {
        Map<String, ServiceDescriptor> byId = Maps.uniqueIndex(descriptors, getNodeIdFunction());

        // remove old ones
        Iterator<Map.Entry<String, BatchProcessor<Entry>>> iterator = processors.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, BatchProcessor<Entry>> entry = iterator.next();

            if (!byId.containsKey(entry.getKey())) {
                iterator.remove();
                entry.getValue().stop();
            }
        }

        Predicate<ServiceDescriptor> predicate = compose(and(not(equalTo(node.getNodeId())), not(in(processors.keySet()))), getNodeIdFunction());
        Iterable<ServiceDescriptor> newDescriptors = filter(descriptors, predicate);

        for (ServiceDescriptor descriptor : newDescriptors) {
            BatchProcessor<Entry> processor = new BatchProcessor<Entry>(descriptor.getNodeId(),
                    new MyBatchHandler(descriptor, httpClient),
                    maxBatchSize,
                    queueSize);

            processor.start();
            processors.put(descriptor.getNodeId(), processor);
        }
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

        public MyBatchHandler(ServiceDescriptor descriptor, HttpClient httpClient)
        {
            this.httpClient = httpClient;

            // TODO: build URI from resource class
            uri = URI.create(descriptor.getProperties().get("http") + "/v1/store");
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
            catch (Exception e) {
                // ignore
            }
        }
    }
}
