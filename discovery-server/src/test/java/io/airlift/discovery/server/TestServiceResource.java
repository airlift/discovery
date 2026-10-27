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

import com.google.common.collect.ImmutableMap;
import io.airlift.node.NodeInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;

import static com.google.common.collect.ImmutableSet.of;
import static io.airlift.discovery.server.DynamicServiceAnnouncement.toServiceWith;
import static org.assertj.core.api.Assertions.assertThat;

public class TestServiceResource
{
    private InMemoryDynamicStore dynamicStore;
    private ServiceResource resource;

    @BeforeEach
    protected void setUp()
    {
        dynamicStore = new InMemoryDynamicStore(new DiscoveryConfig(), new TestingTimeSupplier());
        resource = new ServiceResource(dynamicStore, new InMemoryStaticStore(), new NodeInfo("testing"));
    }

    @Test
    public void testGetByType()
    {
        Id<Node> redNodeId = Id.random();
        DynamicServiceAnnouncement redStorage = new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("key", "1"));
        DynamicServiceAnnouncement redWeb = new DynamicServiceAnnouncement(Id.random(), "web", ImmutableMap.of("key", "2"));
        DynamicAnnouncement red = new DynamicAnnouncement("testing", "alpha", "/a/b/c", of(redStorage, redWeb));

        Id<Node> greenNodeId = Id.random();
        DynamicServiceAnnouncement greenStorage = new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("key", "3"));
        DynamicAnnouncement green = new DynamicAnnouncement("testing", "alpha", "/x/y/z", of(greenStorage));

        Id<Node> blueNodeId = Id.random();
        DynamicServiceAnnouncement blueStorage = new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("key", "4"));
        DynamicAnnouncement blue = new DynamicAnnouncement("testing", "beta", "/a/b/c", of(blueStorage));

        dynamicStore.put(redNodeId, red);
        dynamicStore.put(greenNodeId, green);
        dynamicStore.put(blueNodeId, blue);

        assertThat(resource.getServices("storage")).isEqualTo(new Services("testing", of(
                toServiceWith(redNodeId, red.getLocation(), red.getPool()).apply(redStorage),
                toServiceWith(greenNodeId, green.getLocation(), green.getPool()).apply(greenStorage),
                toServiceWith(blueNodeId, blue.getLocation(), blue.getPool()).apply(blueStorage))));

        assertThat(resource.getServices("web")).isEqualTo(new Services("testing", of(
                toServiceWith(redNodeId, red.getLocation(), red.getPool()).apply(redWeb))));

        assertThat(resource.getServices("unknown")).isEqualTo(new Services("testing", Collections.emptySet()));
    }

    @Test
    public void testGetByTypeAndPool()
    {
        Id<Node> redNodeId = Id.random();
        DynamicServiceAnnouncement redStorage = new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("key", "1"));
        DynamicServiceAnnouncement redWeb = new DynamicServiceAnnouncement(Id.random(), "web", ImmutableMap.of("key", "2"));
        DynamicAnnouncement red = new DynamicAnnouncement("testing", "alpha", "/a/b/c", of(redStorage, redWeb));

        Id<Node> greenNodeId = Id.random();
        DynamicServiceAnnouncement greenStorage = new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("key", "3"));
        DynamicAnnouncement green = new DynamicAnnouncement("testing", "alpha", "/x/y/z", of(greenStorage));

        Id<Node> blueNodeId = Id.random();
        DynamicServiceAnnouncement blueStorage = new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("key", "4"));
        DynamicAnnouncement blue = new DynamicAnnouncement("testing", "beta", "/a/b/c", of(blueStorage));

        dynamicStore.put(redNodeId, red);
        dynamicStore.put(greenNodeId, green);
        dynamicStore.put(blueNodeId, blue);

        assertThat(resource.getServices("storage", "alpha")).isEqualTo(new Services("testing", of(
                toServiceWith(redNodeId, red.getLocation(), red.getPool()).apply(redStorage),
                toServiceWith(greenNodeId, green.getLocation(), green.getPool()).apply(greenStorage))));

        assertThat(resource.getServices("storage", "beta")).isEqualTo(new Services("testing", of(toServiceWith(blueNodeId, blue.getLocation(), blue.getPool()).apply(blueStorage))));

        assertThat(resource.getServices("storage", "unknown")).isEqualTo(new Services("testing", Collections.emptySet()));
    }

    @Test
    public void testGetAll()
    {
        Id<Node> redNodeId = Id.random();
        DynamicServiceAnnouncement redStorage = new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("key", "1"));
        DynamicServiceAnnouncement redWeb = new DynamicServiceAnnouncement(Id.random(), "web", ImmutableMap.of("key", "2"));
        DynamicAnnouncement red = new DynamicAnnouncement("testing", "alpha", "/a/b/c", of(redStorage, redWeb));

        Id<Node> greenNodeId = Id.random();
        DynamicServiceAnnouncement greenStorage = new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("key", "3"));
        DynamicAnnouncement green = new DynamicAnnouncement("testing", "alpha", "/x/y/z", of(greenStorage));

        Id<Node> blueNodeId = Id.random();
        DynamicServiceAnnouncement blueStorage = new DynamicServiceAnnouncement(Id.random(), "storage", ImmutableMap.of("key", "4"));
        DynamicAnnouncement blue = new DynamicAnnouncement("testing", "beta", "/a/b/c", of(blueStorage));

        dynamicStore.put(redNodeId, red);
        dynamicStore.put(greenNodeId, green);
        dynamicStore.put(blueNodeId, blue);

        assertThat(resource.getServices()).isEqualTo(new Services("testing", of(
                toServiceWith(redNodeId, red.getLocation(), red.getPool()).apply(redStorage),
                toServiceWith(redNodeId, red.getLocation(), red.getPool()).apply(redWeb),
                toServiceWith(greenNodeId, green.getLocation(), green.getPool()).apply(greenStorage),
                toServiceWith(blueNodeId, blue.getLocation(), blue.getPool()).apply(blueStorage))));
    }
}
