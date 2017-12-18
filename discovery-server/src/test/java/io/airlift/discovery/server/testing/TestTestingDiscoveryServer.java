/*
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
package io.airlift.discovery.server.testing;

import com.google.common.collect.ImmutableSet;
import io.airlift.discovery.client.Announcement;
import io.airlift.discovery.client.DiscoveryAnnouncementClient;
import io.airlift.discovery.client.DiscoveryLookupClient;
import io.airlift.discovery.client.HttpDiscoveryAnnouncementClient;
import io.airlift.discovery.client.HttpDiscoveryLookupClient;
import io.airlift.discovery.client.ServiceDescriptors;
import io.airlift.discovery.client.ServiceDescriptorsRepresentation;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.jetty.JettyHttpClient;
import io.airlift.node.NodeInfo;
import org.testng.annotations.Test;

import static io.airlift.discovery.client.ServiceAnnouncement.serviceAnnouncement;
import static io.airlift.json.JsonCodec.jsonCodec;
import static org.testng.Assert.assertEquals;

public class TestTestingDiscoveryServer
{
    private static final String ENVIRONMENT = "testing";
    private static final String SERVICE = "taco";

    @Test(timeOut = 30_000)
    public void testServer()
            throws Exception
    {
        try (TestingDiscoveryServer server = new TestingDiscoveryServer(ENVIRONMENT);
                HttpClient httpClient = new JettyHttpClient()) {
            DiscoveryLookupClient lookup = new HttpDiscoveryLookupClient(
                    server::getBaseUrl,
                    new NodeInfo(ENVIRONMENT),
                    jsonCodec(ServiceDescriptorsRepresentation.class),
                    httpClient);

            DiscoveryAnnouncementClient announcement = new HttpDiscoveryAnnouncementClient(
                    server::getBaseUrl,
                    new NodeInfo(ENVIRONMENT),
                    jsonCodec(Announcement.class),
                    httpClient);

            ServiceDescriptors response = lookup.getServices(SERVICE).get();
            assertEquals(response.getServiceDescriptors().size(), 0);

            announcement.announce(ImmutableSet.of(serviceAnnouncement(SERVICE).build())).get();

            response = lookup.getServices(SERVICE).get();
            assertEquals(response.getServiceDescriptors().size(), 1);

            announcement.unannounce().get();

            response = lookup.getServices(SERVICE).get();
            assertEquals(response.getServiceDescriptors().size(), 0);
        }
    }
}
