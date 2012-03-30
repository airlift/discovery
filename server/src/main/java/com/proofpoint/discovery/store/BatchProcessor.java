package com.proofpoint.discovery.store;

import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.proofpoint.log.Logger;
import org.weakref.jmx.Managed;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

public class BatchProcessor<T>
{
    private final static Logger log = Logger.get(BatchProcessor.class);

    private final BatchHandler<T> handler;
    private final int maxBatchSize;
    private final BlockingQueue<T> queue;
    private final String name;

    private ExecutorService executor;
    private volatile Future<?> future;

    private final AtomicLong processedEntries = new AtomicLong();
    private final AtomicLong droppedEntries = new AtomicLong();
    private final AtomicLong errors = new AtomicLong();

    public BatchProcessor(String name, BatchHandler<T> handler, int maxBatchSize, int queueSize)
    {
        Preconditions.checkNotNull(name, "name is null");
        Preconditions.checkNotNull(handler, "handler is null");
        Preconditions.checkArgument(queueSize > 0, "queue size needs to be a positive integer");
        Preconditions.checkArgument(maxBatchSize > 0, "max batch size needs to be a positive integer");

        this.name = name;
        this.handler = handler;
        this.maxBatchSize = maxBatchSize;
        this.queue = new ArrayBlockingQueue<T>(queueSize);
    }

    @PostConstruct
    public synchronized void start()
    {
        if (future == null) {
            executor = Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("batch-processor-" + name + "-%d").build());

            future = executor.submit(new Runnable() {
                public void run()
                {
                    while (!Thread.interrupted()) {
                        final List<T> entries = new ArrayList<T>(maxBatchSize);

                        try {
                            T first = queue.take();
                            entries.add(first);
                            queue.drainTo(entries, maxBatchSize - 1);

                            handler.processBatch(Collections.unmodifiableList(entries));

                            processedEntries.addAndGet(entries.size());
                        }
                        catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                        catch (Throwable t) {
                            errors.incrementAndGet();
                            log.warn(t, "Error handling batch");
                        }

                        // TODO: expose timestamp of last execution via jmx
                    }
                }
            });
        }
    }

    @Managed
    public long getProcessedEntries()
    {
        return processedEntries.get();
    }

    @Managed
    public long getDroppedEntries()
    {
        return droppedEntries.get();
    }

    @Managed
    public long getErrors()
    {
        return errors.get();
    }

    @Managed
    public long getQueueSize()
    {
        return queue.size();
    }

    @PreDestroy
    public synchronized void stop()
    {
        if (future != null) {
            future.cancel(true);
            executor.shutdownNow();

            future = null;
        }
    }

    public void put(T entry)
    {
        Preconditions.checkState(!future.isCancelled(), "Processor is not running");
        Preconditions.checkNotNull(entry, "entry is null");

        while (!queue.offer(entry)) {
            // throw away oldest and try again
            if (queue.poll() != null) {
                droppedEntries.incrementAndGet();
            }
        }
    }

    public static interface BatchHandler<T>
    {
        void processBatch(Collection<T> entries);
    }
}
