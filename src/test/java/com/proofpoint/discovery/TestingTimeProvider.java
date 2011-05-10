package com.proofpoint.discovery;

import com.proofpoint.units.Duration;
import org.joda.time.DateTime;

import javax.inject.Provider;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

class TestingTimeProvider
        implements Provider<DateTime>
{
    private final AtomicLong currentTime = new AtomicLong(System.currentTimeMillis());

    public void add(Duration interval)
    {
        currentTime.addAndGet((long) interval.toMillis());
    }

    public void set(DateTime currentTime)
    {
        this.currentTime.set(currentTime.getMillis());
    }

    public void increment()
    {
        currentTime.incrementAndGet();
    }

    @Override
    public DateTime get()
    {
        return new DateTime(currentTime.get());
    }
}
