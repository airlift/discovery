package com.proofpoint.discovery;

import org.joda.time.DateTime;

import javax.inject.Provider;
import java.util.concurrent.atomic.AtomicReference;

class TestingTimeProvider
        implements Provider<DateTime>
{
    private final AtomicReference<DateTime> currentTime;

    TestingTimeProvider()
    {
        currentTime = new AtomicReference<DateTime>(new DateTime());
    }

    public void set(DateTime currentTime)
    {
        this.currentTime.set(currentTime);
    }

    @Override
    public DateTime get()
    {
        return currentTime.get();
    }
}
