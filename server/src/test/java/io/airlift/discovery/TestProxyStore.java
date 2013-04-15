package io.airlift.discovery;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import com.google.common.collect.ListMultimap;
import com.google.common.util.concurrent.AbstractFuture;
import com.google.inject.Injector;
import com.google.inject.Key;
import io.airlift.discovery.DiscoveryConfig.StringSet;
import io.airlift.discovery.client.DiscoveryException;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.RequestStats;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import org.testng.annotations.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.airlift.json.JsonCodec.jsonCodec;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestProxyStore
{
    @Test
    public void testNoProxy()
    {
        Injector injector = mock(Injector.class);
        ProxyStore proxyStore = new ProxyStore(new DiscoveryConfig(), injector);
        Set<Service> services = ImmutableSet.of(new Service(Id.<Service>random(), Id.<Node>random(), "type", "pool", "/location", ImmutableMap.of("key", "value")));

        assertEquals(proxyStore.filterAndGetAll(services), services);
        assertEquals(proxyStore.get("foo"), null);
        assertEquals(proxyStore.get("foo", "bar"), null);
        verifyNoMoreInteractions(injector);
    }

    @Test
    public void testProxy()
            throws InterruptedException
    {
        Service service1 = new Service(Id.<Service>random(), Id.<Node>random(), "storage", "pool1", "/location/1", ImmutableMap.of("key", "value"));
        Service service2 = new Service(Id.<Service>random(), Id.<Node>random(), "storage", "pool2", "/location/2", ImmutableMap.of("key2", "value2"));
        Service service3 = new Service(Id.<Service>random(), Id.<Node>random(), "customer", "general", "/location/3", ImmutableMap.of("key3", "value3"));

        DiscoveryConfig config = new DiscoveryConfig()
                .setProxyTypes(StringSet.of("storage", "customer", "auth"))
                .setProxyEnvironment("upstream")
                .setProxyUri(URI.create("http://discovery.example.com"));
        Injector injector = mock(Injector.class);
        AsyncHttpClient httpClient = new TestingDiscoveryHttpClient(config, new Service[]{service1, service2, service3});
        when(injector.getInstance(Key.get(AsyncHttpClient.class, ForProxyStore.class))).thenReturn(httpClient);
        ProxyStore proxyStore = new ProxyStore(config, injector);
        Thread.sleep(100);

        Service service4 = new Service(Id.<Service>random(), Id.<Node>random(), "storage", "pool1", "/location/4", ImmutableMap.of("key4", "value4"));
        Service service5 = new Service(Id.<Service>random(), Id.<Node>random(), "auth", "pool3", "/location/5", ImmutableMap.of("key5", "value5"));
        Service service6 = new Service(Id.<Service>random(), Id.<Node>random(), "event", "general", "/location/6", ImmutableMap.of("key6", "value6"));

        assertEquals(proxyStore.filterAndGetAll(ImmutableSet.of(service4, service5, service6)),
                ImmutableSet.of(service1, service2, service3, service6));

        assertEquals(proxyStore.get("storage"), ImmutableSet.of(service1, service2));
        assertEquals(proxyStore.get("customer"), ImmutableSet.of(service3));
        assertEquals(proxyStore.get("auth"), ImmutableSet.<Service>of());
        assertEquals(proxyStore.get("event"), null);

        assertEquals(proxyStore.get("storage", "pool1"), ImmutableSet.of(service1));
        assertEquals(proxyStore.get("storage", "pool2"), ImmutableSet.of(service2));
        assertEquals(proxyStore.get("customer", "general"), ImmutableSet.of(service3));
        assertEquals(proxyStore.get("customer", "pool3"), ImmutableSet.<Service>of());
        assertEquals(proxyStore.get("auth", "pool3"), ImmutableSet.<Service>of());
        assertEquals(proxyStore.get("event", "general"), null);
    }

    @Test(expectedExceptions = DiscoveryException.class,
            expectedExceptionsMessageRegExp = "Expected environment to be upstream, but was mismatch")
    public void testEnvironmentMismatch()
    {
        Service service1 = new Service(Id.<Service>random(), Id.<Node>random(), "storage", "pool1", "/location/1", ImmutableMap.of("key", "value"));
        Service service2 = new Service(Id.<Service>random(), Id.<Node>random(), "storage", "pool2", "/location/2", ImmutableMap.of("key2", "value2"));
        Service service3 = new Service(Id.<Service>random(), Id.<Node>random(), "customer", "general", "/location/3", ImmutableMap.of("key3", "value3"));

        Injector injector = mock(Injector.class);
        AsyncHttpClient httpClient = new TestingDiscoveryHttpClient(new DiscoveryConfig()
                .setProxyTypes(StringSet.of("storage", "customer", "auth"))
                .setProxyEnvironment("mismatch")
                .setProxyUri(URI.create("http://discovery.example.com")), new Service[]{service1, service2, service3});
        when(injector.getInstance(Key.get(AsyncHttpClient.class, ForProxyStore.class))).thenReturn(httpClient);
        new ProxyStore(new DiscoveryConfig()
                .setProxyTypes(StringSet.of("storage", "customer", "auth"))
                .setProxyEnvironment("upstream")
                .setProxyUri(URI.create("http://discovery.example.com")), injector);
    }

    private static class TestingDiscoveryHttpClient implements AsyncHttpClient
    {
        private final DiscoveryConfig config;
        private final ImmutableSet<Service> services;

        public TestingDiscoveryHttpClient(DiscoveryConfig config, Service[] services)
        {
            this.config = config;
            this.services = ImmutableSet.copyOf(services);
        }

        @Override
        public <T, E extends Exception> AsyncHttpResponseFuture<T, E> executeAsync(Request request, ResponseHandler<T, E> responseHandler)
        {
            assertEquals(request.getMethod(), "GET");
            URI uri = request.getUri();
            assertTrue(uri.toString().startsWith("http://discovery.example.com/v1/service/"), "uri " + uri.toString() + " starts with expected prefix");
            String type = uri.toASCIIString().substring(40);
            if (type.endsWith("/")) {
                type = type.substring(0, type.length() - 1);
            }
            assertTrue(config.getProxyTypes().contains(type), "type " + type + " in configured proxy types");

            Builder<Service> builder = ImmutableSet.builder();
            for (Service service : services) {
                if (type.equals(service.getType())) {
                    builder.add(service);
                }
            }
            final Services filteredServices = new Services(config.getProxyEnvironment(), builder.build());

            return new TestingResponseFuture(request, responseHandler, filteredServices);
        }

        @Override
        public <T, E extends Exception> T execute(Request request, ResponseHandler<T, E> responseHandler)
                throws E
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public RequestStats getStats()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close()
        {
        }

        private static class TestingResponseFuture<T, E extends Exception>
                extends AbstractFuture<T>
                implements AsyncHttpResponseFuture<T, E>
        {
            public TestingResponseFuture(Request request, ResponseHandler<T, E> responseHandler, final Services filteredServices)
            {
                try {
                    T result = responseHandler.handle(request, new Response()
                    {
                        @Override
                        public int getStatusCode()
                        {
                            return 200;
                        }

                        @Override
                        public String getStatusMessage()
                        {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public String getHeader(String name)
                        {
                            return null;
                        }

                        @Override
                        public ListMultimap<String, String> getHeaders()
                        {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public long getBytesRead()
                        {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public InputStream getInputStream()
                                throws IOException
                        {
                            return new ByteArrayInputStream(jsonCodec(Services.class).toJson(filteredServices).getBytes("UTF-8"));
                        }
                    });
                    set(result);
                }
                catch (Exception e) {
                    setException(e);
                }
            }

            @Override
            public String getState()
            {
                return "done";
            }

            @Override
            public T checkedGet()
                    throws E
            {
                try {
                    return get();
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return null;
                }
                catch (ExecutionException e) {
                    throw (E) e.getCause();
                }
            }

            @Override
            public T checkedGet(long l, TimeUnit timeUnit)
                    throws TimeoutException, E
            {
                return checkedGet();
            }
        }
    }
}
