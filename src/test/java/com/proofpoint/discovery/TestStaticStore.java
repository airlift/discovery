package com.proofpoint.discovery;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.proofpoint.testing.Assertions.assertEqualsIgnoreOrder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public abstract class TestStaticStore
{
    protected StaticStore store;

    protected abstract StaticStore initializeStore();

    @BeforeMethod
    public void setup()
    {
        store = initializeStore();
    }


    @Test
    public void testEmpty()
    {
        assertTrue(store.getAll().isEmpty(), "store should be empty");
    }

    @Test
    public void testPutSingle()
    {
        Service blue = new Service(Id.<Service>random(), null, "storage", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableMap.of("http", "http://localhost:1111"));

        store.put(blue);
        assertEquals(store.getAll(), ImmutableSet.of(blue));
    }

    @Test
    public void testPutMultiple()
    {
        Service blue = new Service(Id.<Service>random(), null, "storage", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableMap.of("http", "http://localhost:1111"));
        Service red = new Service(Id.<Service>random(), null, "web", "poolA", "/US/West/SC4/rack1/host1/vm1/slot2", ImmutableMap.of("http", "http://localhost:2222"));
        Service green = new Service(Id.<Service>random(), null, "monitoring", "poolA", "/US/West/SC4/rack1/host1/vm1/slot3", ImmutableMap.of("http", "http://localhost:3333"));

        store.put(blue);
        store.put(red);
        store.put(green);

        assertEqualsIgnoreOrder(store.getAll(), ImmutableSet.of(blue, red, green));
    }

    @Test
    public void testGetByType()
    {
        Service blue = new Service(Id.<Service>random(), null, "storage", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableMap.of("http", "http://localhost:1111"));
        Service red = new Service(Id.<Service>random(), null, "storage", "poolB", "/US/West/SC4/rack1/host1/vm1/slot2", ImmutableMap.of("http", "http://localhost:2222"));
        Service green = new Service(Id.<Service>random(), null, "monitoring", "poolA", "/US/West/SC4/rack1/host1/vm1/slot3", ImmutableMap.of("http", "http://localhost:3333"));

        store.put(blue);
        store.put(red);
        store.put(green);

        assertEqualsIgnoreOrder(store.get("storage"), ImmutableSet.of(blue, red));
        assertEqualsIgnoreOrder(store.get("monitoring"), ImmutableSet.of(green));
    }

    @Test
    public void testGetByTypeAndPool()
    {
        Service blue = new Service(Id.<Service>random(), null, "storage", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableMap.of("http", "http://localhost:1111"));
        Service red = new Service(Id.<Service>random(), null, "storage", "poolA", "/US/West/SC4/rack1/host1/vm1/slot2", ImmutableMap.of("http", "http://localhost:2222"));
        Service green = new Service(Id.<Service>random(), null, "monitoring", "poolA", "/US/West/SC4/rack1/host1/vm1/slot3", ImmutableMap.of("http", "http://localhost:3333"));
        Service yellow = new Service(Id.<Service>random(), null, "storage", "poolB", "/US/West/SC4/rack1/host1/vm1/slot3", ImmutableMap.of("http", "http://localhost:4444"));

        store.put(blue);
        store.put(red);
        store.put(green);
        store.put(yellow);

        assertEqualsIgnoreOrder(store.get("storage", "poolA"), ImmutableSet.of(blue, red));
        assertEqualsIgnoreOrder(store.get("monitoring", "poolA"), ImmutableSet.of(green));
        assertEqualsIgnoreOrder(store.get("storage", "poolB"), ImmutableSet.of(yellow));
    }

    @Test
    public void testDelete()
    {
        Service blue = new Service(Id.<Service>random(), null, "storage", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableMap.of("http", "http://localhost:1111"));
        Service red = new Service(Id.<Service>random(), null, "storage", "poolB", "/US/West/SC4/rack1/host1/vm1/slot2", ImmutableMap.of("http", "http://localhost:2222"));
        Service green = new Service(Id.<Service>random(), null, "web", "poolA", "/US/West/SC4/rack1/host1/vm1/slot3", ImmutableMap.of("http", "http://localhost:3333"));

        store.put(blue);
        store.put(red);
        store.put(green);

        assertEqualsIgnoreOrder(store.getAll(), ImmutableSet.of(blue, red, green));

        store.delete(green.getId());

        assertEqualsIgnoreOrder(store.getAll(), ImmutableSet.of(blue, red));
        assertTrue(store.get("web").isEmpty());
        assertTrue(store.get("web", "poolA").isEmpty());
    }
}
