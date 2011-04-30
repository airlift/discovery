package com.proofpoint.discovery;

import com.proofpoint.bootstrap.Bootstrap;
import com.proofpoint.experimental.json.JsonModule;
import com.proofpoint.http.server.HttpServerModule;
import com.proofpoint.jaxrs.JaxrsModule;
import com.proofpoint.jmx.JmxModule;
import com.proofpoint.node.NodeModule;

public class Main
{
    public static void main(String[] args)
            throws Exception
    {
        Bootstrap app = new Bootstrap(new NodeModule(),
                                      new HttpServerModule(),
                                      new JaxrsModule(),
                                      new JsonModule(),
                                      new JmxModule(),
                                      new DiscoveryModule());
        app.initialize();
    }
}