package com.proofpoint.discovery;

import com.google.inject.Injector;
import com.google.inject.Key;
import com.proofpoint.bootstrap.Bootstrap;
import com.proofpoint.discovery.client.Announcer;
import com.proofpoint.discovery.client.DiscoveryModule;
import com.proofpoint.discovery.store.ReplicatedStoreModule;
import com.proofpoint.event.client.HttpEventModule;
import com.proofpoint.http.server.HttpServerModule;
import com.proofpoint.jaxrs.JaxrsModule;
import com.proofpoint.jmx.JmxModule;
import com.proofpoint.jmx.http.rpc.JmxHttpRpcModule;
import com.proofpoint.json.JsonModule;
import com.proofpoint.log.Logger;
import com.proofpoint.node.NodeModule;
import com.proofpoint.tracetoken.TraceTokenModule;
import org.weakref.jmx.guice.MBeanModule;

public class Main
{
    private final static Logger log = Logger.get(Main.class);

    public static void main(String[] args)
            throws Exception
    {
        try {
            Bootstrap app = new Bootstrap(new MBeanModule(),
                                          new NodeModule(),
                                          new HttpServerModule(),
                                          new JaxrsModule(),
                                          new JsonModule(),
                                          new JmxModule(),
                                          new JmxHttpRpcModule(),
                                          new DiscoveryServerModule(),
                                          new HttpEventModule(),
                                          new TraceTokenModule(),
                                          new DiscoveryModule()
                         );

            Injector injector = app.initialize();

            injector.getInstance(Announcer.class).start();
        }
        catch (Exception e) {
            log.error(e);
            // Cassandra prevents the vm from shutting down on its own
            System.exit(1);
        }
    }
}