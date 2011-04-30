package com.proofpoint.discovery;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.proofpoint.node.NodeInfo;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.Set;


@Path("/v1/service")
public class ServiceResource
{
    private final Store store;
    private final NodeInfo node;

    @Inject
    public ServiceResource(Store store, NodeInfo node)
    {
        this.store = store;
        this.node = node;
    }

    @GET
    @Path("{type}/{pool}")
    @Produces(MediaType.APPLICATION_JSON)
    public ServicesRepresentation getServices(@PathParam("type") String type, @PathParam("pool") String pool)
    {
        Set<ServiceDescriptor> descriptors = store.get(type, pool);
        return new ServicesRepresentation(node.getEnvironment(), transform(descriptors));
    }

    private Set<ServiceRepresentation> transform(Set<ServiceDescriptor> descriptors)
    {
        ImmutableSet.Builder<ServiceRepresentation> builder = new ImmutableSet.Builder<ServiceRepresentation>();
        for (ServiceDescriptor descriptor : descriptors) {
            builder.add(new ServiceRepresentation(descriptor.getId(),
                                                     descriptor.getNodeId(),
                                                     descriptor.getType(),
                                                     descriptor.getPool(),
                                                     descriptor.getLocation(),
                                                     descriptor.getProperties()));
        }

        return builder.build();
    }


    @GET
    @Path("{type}")
    @Produces(MediaType.APPLICATION_JSON)
    public ServicesRepresentation getServices(@PathParam("type") String type)
    {
        Set<ServiceDescriptor> descriptors = store.get(type);
        return new ServicesRepresentation(node.getEnvironment(), transform(descriptors));
    }
}
