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

import com.google.inject.Injector;
import io.airlift.bootstrap.Bootstrap;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.discovery.client.DiscoveryModule;
import io.airlift.discovery.server.EmbeddedDiscoveryModule;
import io.airlift.http.server.testing.TestingHttpServer;
import io.airlift.http.server.testing.TestingHttpServerModule;
import io.airlift.jaxrs.JaxrsModule;
import io.airlift.jmx.testing.TestingJmxModule;
import io.airlift.json.JsonModule;
import io.airlift.node.testing.TestingNodeModule;
import org.weakref.jmx.guice.MBeanModule;

import java.net.URI;

public class TestingDiscoveryServer
        implements AutoCloseable
{
    private final LifeCycleManager lifeCycleManager;
    private final TestingHttpServer server;

    public TestingDiscoveryServer(String environment)
    {
        Bootstrap app = new Bootstrap(
                new MBeanModule(),
                new TestingNodeModule(environment),
                new TestingHttpServerModule(),
                new JsonModule(),
                new JaxrsModule(),
                new TestingJmxModule(),
                new DiscoveryModule(),
                new EmbeddedDiscoveryModule());

        Injector injector = app
                .doNotInitializeLogging()
                .setRequiredConfigurationProperty("discovery.store-cache-ttl", "0ms")
                .quiet()
                .initialize();

        lifeCycleManager = injector.getInstance(LifeCycleManager.class);

        server = injector.getInstance(TestingHttpServer.class);
    }

    public URI getBaseUrl()
    {
        return server.getBaseUrl();
    }

    @Override
    public void close()
            throws Exception
    {
        if (lifeCycleManager != null) {
            lifeCycleManager.stop();
        }
    }
}
