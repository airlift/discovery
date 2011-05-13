package com.proofpoint.discovery;

import com.proofpoint.bootstrap.Bootstrap;
import com.proofpoint.cassandra.CassandraModule;
import com.proofpoint.json.JsonModule;
import com.proofpoint.http.server.HttpServerModule;
import com.proofpoint.jaxrs.JaxrsModule;
import com.proofpoint.jmx.JmxModule;
import com.proofpoint.log.Logger;
import com.proofpoint.node.NodeModule;

public class Main
{
    private final static Logger log = Logger.get(Main.class);

    public static void main(String[] args)
            throws Exception
    {
        try {
            Bootstrap app = new Bootstrap(new NodeModule(),
                                          new HttpServerModule(),
                                          new JaxrsModule(),
                                          new JsonModule(),
                                          new JmxModule(),
                                          new DiscoveryModule(),
                                          new CassandraModule());
            app.initialize();
        }
        catch (Exception e) {
            log.error(e);
            // Cassandra prevents the vm from shutting down on its own
            System.exit(1);
        }
    }
}