package com.proofpoint.discovery;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.proofpoint.units.Duration;
import org.joda.time.DateTime;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.inject.Provider;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Collections2.transform;
import static com.google.common.collect.Iterables.concat;
import static com.proofpoint.discovery.DynamicServiceAnnouncement.toServiceWith;
import static com.proofpoint.testing.Assertions.assertEqualsIgnoreOrder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public abstract class TestDynamicStore
{
    private static final Duration MAX_AGE = new Duration(1, TimeUnit.MINUTES);

    protected TestingTimeProvider currentTime;
    protected DynamicStore store;

    protected abstract DynamicStore initializeStore(DiscoveryConfig config, Provider<DateTime> timeProvider);

    @BeforeMethod
    public void setup()
    {
        currentTime = new TestingTimeProvider();
        DiscoveryConfig config = new DiscoveryConfig().setMaxAge(new Duration(1, TimeUnit.MINUTES));
        store = initializeStore(config, currentTime);
    }

    @Test
    public void testEmpty()
    {
        assertTrue(store.getAll().isEmpty(), "store should be empty");
    }

    @Test
    public void testPutSingle()
    {
        Id<Node> nodeId = Id.random();
        DynamicAnnouncement blue = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("http", "http://localhost:1111"))
        ));

        assertTrue(store.put(nodeId, blue));

        assertEquals(store.getAll(), transform(blue.getServiceAnnouncements(), toServiceWith(nodeId, blue.getLocation(), blue.getPool())));
    }

    @Test
    public void testExpires()
    {
        Id<Node> nodeId = Id.random();
        DynamicAnnouncement blue = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("http", "http://localhost:1111"))
        ));

        assertTrue(store.put(nodeId, blue));
        advanceTimeBeyondMaxAge();
        assertEquals(store.getAll(), Collections.<Service>emptySet());
    }

    @Test
    public void testPutMultipleForSameNode()
    {
        Id<Node> nodeId = Id.random();
        DynamicAnnouncement announcement = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("http", "http://localhost:1111")),
                new DynamicServiceAnnouncement(Id.<Service>random(), "web", ImmutableMap.of("http", "http://localhost:2222")),
                new DynamicServiceAnnouncement(Id.<Service>random(), "monitoring", ImmutableMap.of("http", "http://localhost:3333"))
        ));

        assertTrue(store.put(nodeId, announcement));

        assertEqualsIgnoreOrder(store.getAll(), transform(announcement.getServiceAnnouncements(), toServiceWith(nodeId, announcement.getLocation(), announcement.getPool())));
    }

    @Test
    public void testReplace()
    {
        Id<Node> nodeId = Id.random();

        DynamicAnnouncement oldAnnouncement = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("http", "http://localhost:1111"))
        ));

        DynamicAnnouncement newAnnouncement = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot2", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("http", "http://localhost:2222"))
        ));

        store.put(nodeId, oldAnnouncement);
        currentTime.increment();
        store.put(nodeId, newAnnouncement);

        assertEquals(store.getAll(), transform(newAnnouncement.getServiceAnnouncements(), toServiceWith(nodeId, newAnnouncement.getLocation(), newAnnouncement.getPool())));
    }

    @Test
    public void testReplaceExpired()
    {
        Id<Node> nodeId = Id.random();

        DynamicAnnouncement oldAnnouncement = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("http", "http://localhost:1111"))
        ));

        DynamicAnnouncement newAnnouncement = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot2", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("http", "http://localhost:2222"))
        ));

        assertTrue(store.put(nodeId, oldAnnouncement));
        advanceTimeBeyondMaxAge();
        assertTrue(store.put(nodeId, newAnnouncement));

        assertEqualsIgnoreOrder(store.getAll(), transform(newAnnouncement.getServiceAnnouncements(), toServiceWith(nodeId, newAnnouncement.getLocation(), newAnnouncement.getPool())));
    }

    @Test
    public void testPutMultipleForDifferentNodes()
    {
        Id<Node> blueNodeId = Id.random();
        DynamicAnnouncement blue = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("http", "http://localhost:1111"))
        ));

        Id<Node> redNodeId = Id.random();
        DynamicAnnouncement red = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot2", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.<Service>random(), "web", ImmutableMap.of("http", "http://localhost:2222"))
        ));

        Id<Node> greenNodeId = Id.random();
        DynamicAnnouncement green = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot3", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.<Service>random(), "monitoring", ImmutableMap.of("http", "http://localhost:3333"))
        ));

        assertTrue(store.put(blueNodeId, blue));
        assertTrue(store.put(redNodeId, red));
        assertTrue(store.put(greenNodeId, green));

        assertEqualsIgnoreOrder(store.getAll(), concat(
                transform(blue.getServiceAnnouncements(), toServiceWith(blueNodeId, blue.getLocation(), blue.getPool())),
                transform(red.getServiceAnnouncements(), toServiceWith(redNodeId, red.getLocation(), red.getPool())),
                transform(green.getServiceAnnouncements(), toServiceWith(greenNodeId, green.getLocation(), green.getPool()))));
    }

    @Test
    public void testGetByType()
    {
        Id<Node> blueNodeId = Id.random();
        DynamicAnnouncement blue = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("http", "http://localhost:1111"))
        ));

        Id<Node> redNodeId = Id.random();
        DynamicAnnouncement red = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot2", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("http", "http://localhost:2222"))
        ));

        Id<Node> greenNodeId = Id.random();
        DynamicAnnouncement green = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot3", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.<Service>random(), "monitoring", ImmutableMap.of("http", "http://localhost:3333"))
        ));

        assertTrue(store.put(blueNodeId, blue));
        assertTrue(store.put(redNodeId, red));
        assertTrue(store.put(greenNodeId, green));

        assertEqualsIgnoreOrder(store.get("storage"), concat(
                transform(blue.getServiceAnnouncements(), toServiceWith(blueNodeId, blue.getLocation(), blue.getPool())),
                transform(red.getServiceAnnouncements(), toServiceWith(redNodeId, red.getLocation(), red.getPool()))));

        assertEqualsIgnoreOrder(store.get("monitoring"), transform(green.getServiceAnnouncements(), toServiceWith(greenNodeId, green.getLocation(), green.getPool())));
    }

    @Test
    public void testGetByTypeAndPool()
    {
        Id<Node> blueNodeId = Id.random();
        DynamicAnnouncement blue = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("http", "http://localhost:1111"))
        ));

        Id<Node> redNodeId = Id.random();
        DynamicAnnouncement red = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot2", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("http", "http://localhost:2222"))
        ));

        Id<Node> greenNodeId = Id.random();
        DynamicAnnouncement green = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot3", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.<Service>random(), "monitoring", ImmutableMap.of("http", "http://localhost:3333"))
        ));

        Id<Node> yellowNodeId = Id.random();
        DynamicAnnouncement yellow = new DynamicAnnouncement("testing", "poolB", "/US/West/SC4/rack1/host1/vm1/slot4", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("http", "http://localhost:4444"))
        ));

        assertTrue(store.put(blueNodeId, blue));
        assertTrue(store.put(redNodeId, red));
        assertTrue(store.put(greenNodeId, green));
        assertTrue(store.put(yellowNodeId, yellow));

        assertEqualsIgnoreOrder(store.get("storage", "poolA"), concat(
                transform(blue.getServiceAnnouncements(), toServiceWith(blueNodeId, blue.getLocation(), blue.getPool())),
                transform(red.getServiceAnnouncements(), toServiceWith(redNodeId, red.getLocation(), red.getPool()))));

        assertEqualsIgnoreOrder(store.get("monitoring", "poolA"), concat(
                transform(green.getServiceAnnouncements(), toServiceWith(greenNodeId, red.getLocation(), red.getPool()))));

        assertEqualsIgnoreOrder(store.get("storage", "poolB"), concat(
                transform(yellow.getServiceAnnouncements(), toServiceWith(yellowNodeId, red.getLocation(), red.getPool()))));
    }

    @Test
    public void testDelete()
    {
        Id<Node> blueNodeId = Id.random();
        DynamicAnnouncement blue = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("http", "http://localhost:1111")),
                new DynamicServiceAnnouncement(Id.<Service>random(), "web", ImmutableMap.of("http", "http://localhost:2222"))
        ));

        Id<Node> redNodeId = Id.random();
        DynamicAnnouncement red = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot2", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.<Service>random(), "monitoring", ImmutableMap.of("http", "http://localhost:2222"))
        ));

        assertTrue(store.put(blueNodeId, blue));
        assertTrue(store.put(redNodeId, red));

        assertEqualsIgnoreOrder(store.getAll(), concat(
                transform(blue.getServiceAnnouncements(), toServiceWith(blueNodeId, blue.getLocation(), blue.getPool())),
                transform(red.getServiceAnnouncements(), toServiceWith(redNodeId, red.getLocation(), red.getPool()))));

        currentTime.increment();

        assertTrue(store.delete(blueNodeId));

        assertEqualsIgnoreOrder(store.getAll(), transform(red.getServiceAnnouncements(), toServiceWith(redNodeId, red.getLocation(), red.getPool())));

        assertTrue(store.get("storage").isEmpty());
        assertTrue(store.get("web", "poolA").isEmpty());
    }

    @Test
    public void testDeleteThenReInsert()
    {
        Id<Node> redNodeId = Id.random();
        DynamicAnnouncement red = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot2", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.<Service>random(), "monitoring", ImmutableMap.of("http", "http://localhost:2222"))
        ));

        assertTrue(store.put(redNodeId, red));
        assertEqualsIgnoreOrder(store.getAll(), transform(red.getServiceAnnouncements(), toServiceWith(redNodeId, red.getLocation(), red.getPool())));

        currentTime.increment();

        assertTrue(store.delete(redNodeId));

        currentTime.increment();

        assertTrue(store.put(redNodeId, red));

        assertEqualsIgnoreOrder(store.getAll(), transform(red.getServiceAnnouncements(), toServiceWith(redNodeId, red.getLocation(), red.getPool())));
    }

    @Test
    public void testCanHandleLotsOfAnnouncements()
    {
        ImmutableSet.Builder<Service> builder = ImmutableSet.builder();
        for (int i = 0; i < 5000; ++i) {
            Id<Node> id = Id.random();
            DynamicServiceAnnouncement serviceAnnouncement = new DynamicServiceAnnouncement(Id.<Service>random(), "storage", ImmutableMap.of("http", "http://localhost:1111"));
            DynamicAnnouncement announcement = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableSet.of(serviceAnnouncement));

            store.put(id, announcement);
            builder.add(new Service(serviceAnnouncement.getId(),
                                    id,
                                    serviceAnnouncement.getType(),
                                    announcement.getPool(),
                                    announcement.getLocation(),
                                    serviceAnnouncement.getProperties()));
        }

        assertEqualsIgnoreOrder(store.getAll(), builder.build());
    }


    private void advanceTimeBeyondMaxAge()
    {
        currentTime.add(new Duration(MAX_AGE.toMillis() * 2, TimeUnit.MILLISECONDS));
    }
}
