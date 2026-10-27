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
package io.airlift.discovery.server;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
import org.joda.time.DateTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Collections2.transform;
import static com.google.common.collect.Iterables.concat;
import static io.airlift.discovery.server.DynamicServiceAnnouncement.toServiceWith;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class TestDynamicStore
{
    private static final Duration MAX_AGE = new Duration(1, TimeUnit.MINUTES);

    protected TestingTimeSupplier currentTime;
    protected DynamicStore store;

    protected abstract DynamicStore initializeStore(DiscoveryConfig config, Supplier<DateTime> timeSupplier);

    @BeforeEach
    public void setup()
    {
        currentTime = new TestingTimeSupplier();
        DiscoveryConfig config = new DiscoveryConfig()
                .setMaxAge(new Duration(1, TimeUnit.MINUTES))
                .setStoreCacheTtl(new Duration(0, TimeUnit.SECONDS));
        store = initializeStore(config, currentTime);
    }

    @Test
    public void testEmpty()
    {
        assertThat(store.getAll().isEmpty()).as("store should be empty").isTrue();
    }

    @Test
    public void testPutSingle()
    {
        Id<Node> nodeId = Id.random();
        DynamicAnnouncement blue = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:1111"))
        ));

        store.put(nodeId, blue);

        assertThat(store.getAll()).containsExactlyInAnyOrderElementsOf(transform(blue.getServiceAnnouncements(), toServiceWith(nodeId, blue.getLocation(), blue.getPool())));
    }

    @Test
    public void testExpires()
    {
        Id<Node> nodeId = Id.random();
        DynamicAnnouncement blue = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:1111"))
        ));

        store.put(nodeId, blue);
        advanceTimeBeyondMaxAge();
        assertThat(store.getAll()).isEqualTo(Collections.<Service>emptySet());
    }

    @Test
    public void testPutMultipleForSameNode()
    {
        Id<Node> nodeId = Id.random();
        DynamicAnnouncement announcement = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:1111")),
                new DynamicServiceAnnouncement(Id.random(), "web", ImmutableMap.of("http", "http://localhost:2222")),
                new DynamicServiceAnnouncement(Id.random(), "monitoring", ImmutableMap.of("http", "http://localhost:3333"))
        ));

        store.put(nodeId, announcement);

        assertThat(store.getAll()).containsExactlyInAnyOrderElementsOf(transform(announcement.getServiceAnnouncements(), toServiceWith(nodeId, announcement.getLocation(), announcement.getPool())));
    }

    @Test
    public void testReplace()
    {
        Id<Node> nodeId = Id.random();

        DynamicAnnouncement oldAnnouncement = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:1111"))
        ));

        DynamicAnnouncement newAnnouncement = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot2", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:2222"))
        ));

        store.put(nodeId, oldAnnouncement);
        currentTime.increment();
        store.put(nodeId, newAnnouncement);

        assertThat(store.getAll()).containsExactlyInAnyOrderElementsOf(transform(newAnnouncement.getServiceAnnouncements(), toServiceWith(nodeId, newAnnouncement.getLocation(), newAnnouncement.getPool())));
    }

    @Test
    public void testReplaceExpired()
    {
        Id<Node> nodeId = Id.random();

        DynamicAnnouncement oldAnnouncement = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:1111"))
        ));

        DynamicAnnouncement newAnnouncement = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot2", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:2222"))
        ));

        store.put(nodeId, oldAnnouncement);
        advanceTimeBeyondMaxAge();
        store.put(nodeId, newAnnouncement);

        assertThat(store.getAll()).containsExactlyInAnyOrderElementsOf(transform(newAnnouncement.getServiceAnnouncements(), toServiceWith(nodeId, newAnnouncement.getLocation(), newAnnouncement.getPool())));
    }

    @Test
    public void testPutMultipleForDifferentNodes()
    {
        Id<Node> blueNodeId = Id.random();
        DynamicAnnouncement blue = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:1111"))
        ));

        Id<Node> redNodeId = Id.random();
        DynamicAnnouncement red = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot2", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "web", ImmutableMap.of("http", "http://localhost:2222"))
        ));

        Id<Node> greenNodeId = Id.random();
        DynamicAnnouncement green = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot3", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "monitoring", ImmutableMap.of("http", "http://localhost:3333"))
        ));

        store.put(blueNodeId, blue);
        store.put(redNodeId, red);
        store.put(greenNodeId, green);

        assertThat(store.getAll()).containsExactlyInAnyOrderElementsOf(concat(
                transform(blue.getServiceAnnouncements(), toServiceWith(blueNodeId, blue.getLocation(), blue.getPool())),
                transform(red.getServiceAnnouncements(), toServiceWith(redNodeId, red.getLocation(), red.getPool())),
                transform(green.getServiceAnnouncements(), toServiceWith(greenNodeId, green.getLocation(), green.getPool()))));
    }

    @Test
    public void testGetByType()
    {
        Id<Node> blueNodeId = Id.random();
        DynamicAnnouncement blue = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:1111"))
        ));

        Id<Node> redNodeId = Id.random();
        DynamicAnnouncement red = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot2", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:2222"))
        ));

        Id<Node> greenNodeId = Id.random();
        DynamicAnnouncement green = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot3", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "monitoring", ImmutableMap.of("http", "http://localhost:3333"))
        ));

        store.put(blueNodeId, blue);
        store.put(redNodeId, red);
        store.put(greenNodeId, green);

        assertThat(store.get("storage")).containsExactlyInAnyOrderElementsOf(concat(
                transform(blue.getServiceAnnouncements(), toServiceWith(blueNodeId, blue.getLocation(), blue.getPool())),
                transform(red.getServiceAnnouncements(), toServiceWith(redNodeId, red.getLocation(), red.getPool()))));

        assertThat(store.get("monitoring")).containsExactlyInAnyOrderElementsOf(transform(green.getServiceAnnouncements(), toServiceWith(greenNodeId, green.getLocation(), green.getPool())));
    }

    @Test
    public void testGetByTypeAndPool()
    {
        Id<Node> blueNodeId = Id.random();
        DynamicAnnouncement blue = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:1111"))
        ));

        Id<Node> redNodeId = Id.random();
        DynamicAnnouncement red = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot2", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:2222"))
        ));

        Id<Node> greenNodeId = Id.random();
        DynamicAnnouncement green = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot3", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "monitoring", ImmutableMap.of("http", "http://localhost:3333"))
        ));

        Id<Node> yellowNodeId = Id.random();
        DynamicAnnouncement yellow = new DynamicAnnouncement("testing", "poolB", "/US/West/SC4/rack1/host1/vm1/slot4", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:4444"))
        ));

        store.put(blueNodeId, blue);
        store.put(redNodeId, red);
        store.put(greenNodeId, green);
        store.put(yellowNodeId, yellow);

        assertThat(store.get("storage", "poolA")).containsExactlyInAnyOrderElementsOf(concat(
                transform(blue.getServiceAnnouncements(), toServiceWith(blueNodeId, blue.getLocation(), blue.getPool())),
                transform(red.getServiceAnnouncements(), toServiceWith(redNodeId, red.getLocation(), red.getPool()))));

        assertThat(store.get("monitoring", "poolA")).containsExactlyInAnyOrderElementsOf(transform(green.getServiceAnnouncements(), toServiceWith(greenNodeId, red.getLocation(), red.getPool())));

        assertThat(store.get("storage", "poolB")).containsExactlyInAnyOrderElementsOf(transform(yellow.getServiceAnnouncements(), toServiceWith(yellowNodeId, red.getLocation(), red.getPool())));
    }

    @Test
    public void testDelete()
    {
        Id<Node> blueNodeId = Id.random();
        DynamicAnnouncement blue = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:1111")),
                new DynamicServiceAnnouncement(Id.random(), "web", ImmutableMap.of("http", "http://localhost:2222"))
        ));

        Id<Node> redNodeId = Id.random();
        DynamicAnnouncement red = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot2", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "monitoring", ImmutableMap.of("http", "http://localhost:2222"))
        ));

        store.put(blueNodeId, blue);
        store.put(redNodeId, red);

        assertThat(store.getAll()).containsExactlyInAnyOrderElementsOf(concat(
                transform(blue.getServiceAnnouncements(), toServiceWith(blueNodeId, blue.getLocation(), blue.getPool())),
                transform(red.getServiceAnnouncements(), toServiceWith(redNodeId, red.getLocation(), red.getPool()))));

        currentTime.increment();

        store.delete(blueNodeId);

        assertThat(store.getAll()).containsExactlyInAnyOrderElementsOf(transform(red.getServiceAnnouncements(), toServiceWith(redNodeId, red.getLocation(), red.getPool())));

        assertThat(store.get("storage").isEmpty()).isTrue();
        assertThat(store.get("web", "poolA").isEmpty()).isTrue();
    }

    @Test
    public void testDeleteThenReInsert()
    {
        Id<Node> redNodeId = Id.random();
        DynamicAnnouncement red = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot2", ImmutableSet.of(
                new DynamicServiceAnnouncement(Id.random(), "monitoring", ImmutableMap.of("http", "http://localhost:2222"))
        ));

        store.put(redNodeId, red);
        assertThat(store.getAll()).containsExactlyInAnyOrderElementsOf(transform(red.getServiceAnnouncements(), toServiceWith(redNodeId, red.getLocation(), red.getPool())));

        currentTime.increment();

        store.delete(redNodeId);

        currentTime.increment();

        store.put(redNodeId, red);

        assertThat(store.getAll()).containsExactlyInAnyOrderElementsOf(transform(red.getServiceAnnouncements(), toServiceWith(redNodeId, red.getLocation(), red.getPool())));
    }

    @Test
    public void testCanHandleLotsOfAnnouncements()
    {
        ImmutableSet.Builder<Service> builder = ImmutableSet.builder();
        for (int i = 0; i < 5000; ++i) {
            Id<Node> id = Id.random();
            DynamicServiceAnnouncement serviceAnnouncement = new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("http", "http://localhost:1111"));
            DynamicAnnouncement announcement = new DynamicAnnouncement("testing", "poolA", "/US/West/SC4/rack1/host1/vm1/slot1", ImmutableSet.of(serviceAnnouncement));

            store.put(id, announcement);
            builder.add(new Service(
                    serviceAnnouncement.getId(),
                    id,
                    serviceAnnouncement.getType(),
                    announcement.getPool(),
                    announcement.getLocation(),
                    serviceAnnouncement.getProperties()));
        }

        assertThat(store.getAll()).containsExactlyInAnyOrderElementsOf(builder.build());
    }

    private void advanceTimeBeyondMaxAge()
    {
        currentTime.add(new Duration(MAX_AGE.toMillis() * 2, TimeUnit.MILLISECONDS));
    }
}
