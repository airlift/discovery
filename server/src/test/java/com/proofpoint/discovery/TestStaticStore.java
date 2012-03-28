package com.proofpoint.discovery;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.inject.Provider;
import java.util.Set;

import static com.proofpoint.testing.Assertions.assertEqualsIgnoreOrder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public abstract class TestStaticStore
{
    private static final Service BLUE = new Service(Id.<Service>random(), null, "storage", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableMap.of("http", "http://localhost:1111"));
    private static final Service RED = new Service(Id.<Service>random(), null, "storage", "poolB", "/US/West/SC4/rack1/host1/vm1/slot2", ImmutableMap.of("http", "http://localhost:2222"));
    private static final Service GREEN = new Service(Id.<Service>random(), null, "monitoring", "poolA", "/US/West/SC4/rack1/host1/vm1/slot3", ImmutableMap.of("http", "http://localhost:3333"));
    private static final Service YELLOW = new Service(Id.<Service>random(), null, "storage", "poolB", "/US/West/SC4/rack1/host1/vm1/slot3", ImmutableMap.of("http", "http://localhost:4444"));

    protected StaticStore store;
    protected TestingTimeProvider currentTime;

    protected abstract StaticStore initializeStore(Provider<DateTime> timeProvider);

    @BeforeMethod
    public void setup()
    {
        currentTime = new TestingTimeProvider();
        store = initializeStore(currentTime);
    }

    @Test
    public void testEmpty()
    {
        assertTrue(store.getAll().isEmpty(), "store should be empty");
    }

    @Test
    public void testPutSingle()
    {
        store.put(BLUE);
        assertEquals(store.getAll(), ImmutableSet.of(BLUE));
    }

    @Test
    public void testPutMultiple()
    {
        store.put(BLUE);
        store.put(RED);
        store.put(GREEN);

        assertEqualsIgnoreOrder(store.getAll(), ImmutableSet.of(BLUE, RED, GREEN));
    }

    @Test
    public void testGetByType()
    {
        store.put(BLUE);
        store.put(RED);
        store.put(GREEN);

        assertEqualsIgnoreOrder(store.get("storage"), ImmutableSet.of(BLUE, RED));
        assertEqualsIgnoreOrder(store.get("monitoring"), ImmutableSet.of(GREEN));
    }

    @Test
    public void testGetByTypeAndPool()
    {
        store.put(BLUE);
        store.put(RED);
        store.put(GREEN);
        store.put(YELLOW);

        assertEqualsIgnoreOrder(store.get("storage", "poolA"), ImmutableSet.of(BLUE));
        assertEqualsIgnoreOrder(store.get("monitoring", "poolA"), ImmutableSet.of(GREEN));
        assertEqualsIgnoreOrder(store.get("storage", "poolB"), ImmutableSet.of(RED, YELLOW));
    }

    @Test
    public void testDelete()
    {
        store.put(BLUE);
        store.put(RED);
        store.put(GREEN);

        assertEqualsIgnoreOrder(store.getAll(), ImmutableSet.of(BLUE, RED, GREEN));

        currentTime.increment();

        store.delete(GREEN.getId());

        assertEqualsIgnoreOrder(store.getAll(), ImmutableSet.of(BLUE, RED));
        assertTrue(store.get("web").isEmpty());
        assertTrue(store.get("web", "poolA").isEmpty());
    }

    @Test
    public void testCanHandleLotsOfServices()
    {
        ImmutableSet.Builder<Service> builder = ImmutableSet.builder();
        for (int i = 0; i < 5000; ++i) {
            builder.add(new Service(Id.<Service>random(), null, "storage", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableMap.of("http", "http://localhost:1111")));
        }
        Set<Service> services = builder.build();

        for (Service service : services) {
            store.put(service);
        }

        assertEquals(store.getAll(), services);
    }
}
